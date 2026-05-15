using MongoDB.Bson;
using MongoDB.Driver;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Buffers;
using System.Collections.Generic;
#if NET8_0_OR_GREATER
using System.Collections.Frozen;
#endif
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence.Mongo
{
    public interface IMongoPersistence : IPersistence
    {
        Task InitAsync(CancellationToken cancellationToken);

        void Init();

        Task DropAsync(CancellationToken cancellationToken);
    }

    public class MongoPersistence : MongoPersistence<MongoChunk>
    {
        public MongoPersistence(MongoPersistenceOptions options) : base(options)
        {
        }
    }

    public class MongoPersistence<TChunk> : IMongoPersistence, IEnhancedPersistence, IPartitionPersistenceSync
        where TChunk : class, IMongoChunk, new()
    {
        private IMongoDatabase _partitionsDb;
        private IMongoDatabase _countersDb;

        private IMongoCollection<TChunk> _chunks;
        private IMongoCollection<Counter> _counters;
        private readonly IMongoPayloadSerializer _mongoPayloadSerializer;
        private readonly MongoPersistenceOptions _options;
        private readonly INStoreLogger _logger;

        private long _sequence = 0;

        /// <summary>
        /// Index for partitionId+Index, used for concurrency check.
        /// </summary>
        private const string PartitionIndexIdx = "partition_index";

        /// <summary>
        /// Index on partitionId+operationId, for idempotency on operations
        /// </summary>
        private const string PartitionOperationIdx = "partition_operation";

#if NET8_0_OR_GREATER
        private static readonly FrozenSet<string> IndexNames = FrozenSet.ToFrozenSet(new[] { PartitionIndexIdx, PartitionOperationIdx, "_id_" });
#endif

        public bool SupportsFillers => true;

        public MongoPersistence(MongoPersistenceOptions options)
        {
            if (options == null || !options.IsValid())
            {
                throw new MongoPersistenceException("Invalid options");
            }

            _options = options;

            var partitionsBuild = new MongoUrlBuilder(options.PartitionsConnectionString);
            _logger = options.LoggerFactory.CreateLogger(
                $"Mongo-{String.Join(",", partitionsBuild.Servers.Select(s => $"{s.Host}:{s.Port}"))}, {options.PartitionsCollectionName}");

            _mongoPayloadSerializer = options.MongoPayloadSerializer ?? new TypeSystemMongoPayloadSerializer();
            Connect();
        }

        private void Connect()
        {
            var partitionsBuild = new MongoUrlBuilder(_options.PartitionsConnectionString);
            _options.CustomizePartitionSettings(partitionsBuild);

            var settings = MongoClientSettings.FromUrl(partitionsBuild.ToMongoUrl());

            //The caller has the option to force the use of other link provider due to customization
            //of the partition client settings. 
            _options.CustomizePartitionClientSettings(settings);

            // IMPORTANT: MongoClient is thread-safe and should be stored as a singleton or reused.
            // MongoClient internally maintains a connection pool. Creating multiple MongoClient instances
            // will create separate connection pools, which is inefficient.
            // Consider using CreateClientFunction to provide a cached/singleton MongoClient instance.
            var partitionsClient = _options.CreateClientFunction(settings);

            this._partitionsDb = partitionsClient.GetDatabase(partitionsBuild.DatabaseName);

            if (_options.SequenceConnectionString == null)
            {
                this._countersDb = _partitionsDb;
            }
            else
            {
                var countersUrlBuilder = new MongoUrlBuilder(_options.SequenceConnectionString);
                _options.CustomizeSequenceSettings(countersUrlBuilder);

                var countersSettings = MongoClientSettings.FromUrl(countersUrlBuilder.ToMongoUrl());
                _options.CustomizeSequenceClientSettings(countersSettings);

                var countersClient = _options.CreateClientFunction(countersSettings);
                this._countersDb = countersClient.GetDatabase(countersUrlBuilder.DatabaseName);
            }
        }

        /// <summary>
        /// Configures FindOptions with cursor batch size if specified in options.
        /// This centralizes batch size configuration to ensure consistent behavior across all read operations.
        /// </summary>
        private void ConfigureFindOptions(FindOptionsBase findOptions)
        {
            if (_options.CursorBatchSize.HasValue)
            {
                findOptions.BatchSize = _options.CursorBatchSize.Value;
            }
        }

        public async Task DropAsync(CancellationToken cancellationToken)
        {
            await ResetCollectionAsync(
                    _partitionsDb,
                    _options.PartitionsCollectionName,
                    _partitionsDb.GetCollection<TChunk>(_options.PartitionsCollectionName),
                    cancellationToken)
                .ConfigureAwait(false);

            await ResetCollectionAsync(
                    _countersDb,
                    _options.SequenceCollectionName,
                    _countersDb.GetCollection<Counter>(_options.SequenceCollectionName),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        private async Task ResetCollectionAsync<TDocument>(
            IMongoDatabase database,
            string collectionName,
            IMongoCollection<TDocument> collection,
            CancellationToken cancellationToken)
        {
            try
            {
                await database
                    .DropCollectionAsync(collectionName, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (MongoCommandException ex) when (IsNamespaceMissing(ex))
            {
                // Treat missing collections as already clean.
            }
            catch (MongoCommandException ex) when (CanDeleteInsteadOfDrop(ex))
            {
                _logger.LogDebug(
                    "DropCollectionAsync is not permitted for {CollectionName}; deleting documents instead. {Message}",
                    collectionName,
                    ex.Message);

                await collection
                    .DeleteManyAsync(Builders<TDocument>.Filter.Empty, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private static bool IsNamespaceMissing(MongoCommandException ex)
        {
            return ex.Code == 26 ||
                   string.Equals(ex.CodeName, "NamespaceNotFound", StringComparison.OrdinalIgnoreCase);
        }

        private static bool CanDeleteInsteadOfDrop(MongoCommandException ex)
        {
            if (ex.Code == 13)
            {
                return true;
            }

            return ex.Message.IndexOf("requires authentication", StringComparison.OrdinalIgnoreCase) >= 0 ||
                   ex.Message.IndexOf("not authorized", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        public void Drop()
        {
            this._partitionsDb.DropCollection(_options.PartitionsCollectionName);
            this._countersDb.DropCollection(_options.SequenceCollectionName);
        }

        public async Task ReadForwardAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            if (limit <= 0)
            {
                await CompleteEmptySubscription(fromLowerIndexInclusive, subscription, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Ascending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }
            ConfigureFindOptions(options);

            await PushToSubscriber(
                fromLowerIndexInclusive,
                subscription,
                options,
                filter,
                false,
                cancellationToken).ConfigureAwait(false);
        }

        public async Task ReadForwardMultiplePartitionsAsync(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken)
        {
            
            var (filter, options) = CreateFilterAndOptionsForReadForwardMultiplePartition(partitionIdsList, fromLowerIndexInclusive, toUpperIndexInclusive);
            ConfigureFindOptions(options);

            await PushToSubscriber(
                fromLowerIndexInclusive,
                subscription,
                options,
                filter,
                false,
                cancellationToken).ConfigureAwait(false);
        }

        private static (FilterDefinition<TChunk>, FindOptions<TChunk>)
                CreateFilterAndOptionsForReadForwardMultiplePartition(IEnumerable<string> partitionIdsList, long fromLowerIndexInclusive, long toUpperIndexInclusive)
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.In(x => x.PartitionId, partitionIdsList),
                Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );
            var sort = Builders<TChunk>.Sort.Combine(
                Builders<TChunk>.Sort.Ascending(x => x.PartitionId),
                Builders<TChunk>.Sort.Ascending(x => x.Index)
            );
            var options = new FindOptions<TChunk>() { Sort = sort };

            return (filter, options);
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
            IEnumerable<string> partitionIdsList,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var (filter, options) = CreateFilterAndOptionsForReadForwardMultiplePartition(partitionIdsList, fromLowerIndexInclusive, toUpperIndexInclusive);

            using var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false);
            while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (var chunk in cursor.Current)
                {
                    _mongoPayloadSerializer.ApplyDeserialization(chunk);
                    yield return chunk;
                }
            }
        }
#endif

        public async Task ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                return;
            }

            // Validate partition requests
            ValidatePartitionRequest(requests, nameof(partitionRequests));

            // Build filter for each partition request and combine them with OR
            var requestCount = requests.Count;
            var partitionFiltersArray = ArrayPool<FilterDefinition<TChunk>>.Shared.Rent(requestCount);
            try
            {
                var (filter, options) = PrepareFilteringForMultiPartitionRequest(requests, requestCount, partitionFiltersArray);

                // Use the minimum from index as the starting position for subscription
                var startIndex = requests.Min(r => r.FromPartitionIndexInclusive);

                await PushToSubscriber(
                    startIndex,
                    subscription,
                    options,
                    filter,
                    false,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<FilterDefinition<TChunk>>.Shared.Return(partitionFiltersArray, clearArray: false);
            }
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                yield break;
            }

            // Validate partition requests
            ValidatePartitionRequest(requests, nameof(partitionRequests));

            // Build filter for each partition request and combine them with OR
            var requestCount = requests.Count;
            var partitionFiltersArray = ArrayPool<FilterDefinition<TChunk>>.Shared.Rent(requestCount);
            try
            {
                var (filter, options) = PrepareFilteringForMultiPartitionRequest(requests, requestCount, partitionFiltersArray);
                ConfigureFindOptions(options);

                using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
                {
                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        foreach (var chunk in cursor.Current)
                        {
                            _mongoPayloadSerializer.ApplyDeserialization(chunk);
                            yield return chunk;
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<FilterDefinition<TChunk>>.Shared.Return(partitionFiltersArray, clearArray: false);
            }
        }
#endif

        public async Task ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                return;
            }

            // Validate partition requests
            ValidatePartitionRequest(requests, nameof(partitionRequests));

            // Build filter for each partition request and combine them with OR
            var requestCount = requests.Count;
            var partitionFiltersArray = ArrayPool<FilterDefinition<TChunk>>.Shared.Rent(requestCount);
            try
            {
                var (filter, options) = PrepareFilteringForMultiPartitionBackwardRequest(requests, requestCount, partitionFiltersArray);
                ConfigureFindOptions(options);

                // Use the maximum upper index as the starting position for subscription
                var startIndex = requests.Max(r => r.ToPartitionIndexInclusive);

                await PushToSubscriber(
                    startIndex,
                    subscription,
                    options,
                    filter,
                    false,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<FilterDefinition<TChunk>>.Shared.Return(partitionFiltersArray, clearArray: false);
            }
        }

#if NET8_0_OR_GREATER
        public async IAsyncEnumerable<IChunk> ReadManyBackwardAsync(
            IEnumerable<PartitionReadRequest> partitionRequests,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (partitionRequests is null)
            {
                throw new ArgumentNullException(nameof(partitionRequests));
            }

            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                yield break;
            }

            // Validate partition requests
            ValidatePartitionRequest(requests, nameof(partitionRequests));

            // Build filter for each partition request and combine them with OR
            var requestCount = requests.Count;
            var partitionFiltersArray = ArrayPool<FilterDefinition<TChunk>>.Shared.Rent(requestCount);
            try
            {
                var (filter, options) = PrepareFilteringForMultiPartitionBackwardRequest(requests, requestCount, partitionFiltersArray);
                ConfigureFindOptions(options);

                using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
                {
                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        foreach (var chunk in cursor.Current)
                        {
                            _mongoPayloadSerializer.ApplyDeserialization(chunk);
                            yield return chunk;
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<FilterDefinition<TChunk>>.Shared.Return(partitionFiltersArray, clearArray: false);
            }
        }
#endif

        private static (FilterDefinition<TChunk>, FindOptions<TChunk>) PrepareFilteringForMultiPartitionBackwardRequest(List<PartitionReadRequest> requests, int requestCount, FilterDefinition<TChunk>[] partitionFiltersArray)
        {
            for (int i = 0; i < requestCount; i++)
            {
                var request = requests[i];
                partitionFiltersArray[i] = Builders<TChunk>.Filter.And(
                    Builders<TChunk>.Filter.Eq(x => x.PartitionId, request.PartitionId),
                    Builders<TChunk>.Filter.Gte(x => x.Index, request.FromPartitionIndexInclusive),
                    Builders<TChunk>.Filter.Lte(x => x.Index, request.ToPartitionIndexInclusive)
                );
            }

            var filter = Builders<TChunk>.Filter.Or(partitionFiltersArray.AsSpan(0, requestCount).ToArray());

            // Sort by partitionId ascending first, then by index descending for backward reading
            var sort = Builders<TChunk>.Sort.Combine(
                Builders<TChunk>.Sort.Ascending(x => x.PartitionId),
                Builders<TChunk>.Sort.Descending(x => x.Index)
            );
            var options = new FindOptions<TChunk>() { Sort = sort };

            return (filter, options);
        }

        private static void ValidatePartitionRequest(List<PartitionReadRequest> requests, string propertyName)
        {
            foreach (var request in requests)
            {
                if (string.IsNullOrWhiteSpace(request.PartitionId))
                {
                    throw new ArgumentException("PartitionId cannot be null or whitespace", propertyName);
                }

                if (request.ToPartitionIndexInclusive < request.FromPartitionIndexInclusive)
                {
                    throw new ArgumentException(
                        $"ToPartitionIndexInclusive ({request.ToPartitionIndexInclusive}) must be >= FromPartitionIndexInclusive ({request.FromPartitionIndexInclusive}) for partition {request.PartitionId}",
                        propertyName);
                }
            }
        }

        public async Task<IReadOnlyDictionary<string, IChunk>> ReadLastChunkForPartitionsAsync(
            IEnumerable<string> partitionIds,
            CancellationToken cancellationToken)
        {
            if (partitionIds is null)
            {
                throw new ArgumentNullException(nameof(partitionIds));
            }

            var partitionList = partitionIds.Where(p => !string.IsNullOrWhiteSpace(p)).Distinct().ToList();
            if (!partitionList.Any())
            {
                return new Dictionary<string, IChunk>();
            }

            // Two-stage approach for optimal performance:
            //
            // Stage 1: Use $group with $max to find the maximum index for each partition.
            //          This is very fast because MongoDB can use a covered query on the index
            //          (PartitionId, Index) - it only reads index entries, not full documents.
            //
            // Stage 2: Fetch the actual documents using exact (PartitionId, Index) lookups.
            //          This uses point queries on the compound index, which are O(log n).
            //
            // Why this is better than $sort + $group with $first:
            // - $sort before $group must scan and sort ALL matching documents
            // - $max only needs to find the maximum value per group (index scan)
            // - Stage 2 fetches exactly N documents (one per partition) with point lookups

            // Stage 1: Get max index per partition using covered index query
            var maxIndexPipeline = new[]
            {
                new BsonDocument("$match", new BsonDocument("PartitionId", new BsonDocument("$in", new BsonArray(partitionList)))),
                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", "$PartitionId" },
                    { "maxIndex", new BsonDocument("$max", "$Index") }
                })
            };

            var maxIndices = new Dictionary<string, long>();
            using (var cursor = await _chunks.AggregateAsync<BsonDocument>(maxIndexPipeline, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    foreach (var doc in cursor.Current)
                    {
                        var partitionId = doc["_id"].AsString;
                        var maxIndex = doc["maxIndex"].AsInt64;
                        maxIndices[partitionId] = maxIndex;
                    }
                }
            }

            if (maxIndices.Count == 0)
            {
                return new Dictionary<string, IChunk>();
            }

            // Stage 2: Fetch actual documents using compound index point lookups
            // Build an OR filter for each (PartitionId, Index) pair - each is a point lookup
            var filters = maxIndices.Select(kv =>
                Builders<TChunk>.Filter.And(
                    Builders<TChunk>.Filter.Eq(x => x.PartitionId, kv.Key),
                    Builders<TChunk>.Filter.Eq(x => x.Index, kv.Value)
                )
            ).ToList();

            var combinedFilter = Builders<TChunk>.Filter.Or(filters);

            var result = new Dictionary<string, IChunk>();
            using (var cursor = await _chunks.FindAsync(combinedFilter, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    foreach (var chunk in cursor.Current)
                    {
                        _mongoPayloadSerializer.ApplyDeserialization(chunk);
                        result[chunk.PartitionId] = chunk;
                    }
                }
            }

            return result;
        }

        private static (FilterDefinition<TChunk>, FindOptions<TChunk>) PrepareFilteringForMultiPartitionRequest(List<PartitionReadRequest> requests, int requestCount, FilterDefinition<TChunk>[] partitionFiltersArray)
        {
            for (int i = 0; i < requestCount; i++)
            {
                var request = requests[i];
                partitionFiltersArray[i] = Builders<TChunk>.Filter.And(
                    Builders<TChunk>.Filter.Eq(x => x.PartitionId, request.PartitionId),
                    Builders<TChunk>.Filter.Gte(x => x.Index, request.FromPartitionIndexInclusive),
                    Builders<TChunk>.Filter.Lte(x => x.Index, request.ToPartitionIndexInclusive)
                );
            }

            var filter = Builders<TChunk>.Filter.Or(partitionFiltersArray.AsSpan(0, requestCount).ToArray());

            //Really important in all the multi partition read. If you simply sort by index, this will really make mongodb sad because
            //it must first scan all partition then sort by index. By sorting first by partitionId we help mongo to read sequentially the data
            //because it will simply sort by partition id, the main filter, then by index.
            var sort = Builders<TChunk>.Sort.Combine(
                Builders<TChunk>.Sort.Ascending(x => x.PartitionId),
                Builders<TChunk>.Sort.Ascending(x => x.Index)
            );
            var options = new FindOptions<TChunk>() { Sort = sort };

            return (filter, options);
        }

        /// <summary>
        /// Pushes a result of a query to a subscriber handling all the logic.
        /// </summary>
        /// <param name="start"></param>
        /// <param name="subscription"></param>
        /// <param name="options"></param>
        /// <param name="filter"></param>
        /// <param name="broadcastPosition"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task PushToSubscriber(
            long start,
            ISubscription subscription,
            FindOptions<TChunk> options,
            FilterDefinition<TChunk> filter,
            bool broadcastPosition,
            CancellationToken cancellationToken)
        {
            long positionOrIndex = 0;
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
                {
                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        foreach (var b in cursor.Current)
                        {
                            positionOrIndex = broadcastPosition ? b.Position : b.Index;
                            _mongoPayloadSerializer.ApplyDeserialization(b);
                            if (!await subscription.OnNextAsync(b).ConfigureAwait(false))
                            {
                                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                }

                await subscription.CompletedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogDebug("PushToSubscriber cancelled at {PositionOrIndex}: {Message}",
                    positionOrIndex,
                    ex.Message);
                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error During PushToSubscriber: {e.Message}.\n{e.StackTrace}");
                await subscription.OnErrorAsync(positionOrIndex, e).ConfigureAwait(false);
            }
        }

        public async Task ReadBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            if (limit <= 0)
            {
                await CompleteEmptySubscription(fromUpperIndexInclusive, subscription, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive),
                Builders<TChunk>.Filter.Gte(x => x.Index, toLowerIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }
            ConfigureFindOptions(options);

            await PushToSubscriber(
                fromUpperIndexInclusive,
                subscription,
                options,
                filter,
                false,
                cancellationToken
            ).ConfigureAwait(false);
        }

        private static async Task CompleteEmptySubscription(
            long start,
            ISubscription subscription,
            CancellationToken cancellationToken)
        {
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await subscription.CompletedAsync(start).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await subscription.StoppedAsync(start).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await subscription.OnErrorAsync(start, e).ConfigureAwait(false);
            }
        }

        public async Task<IChunk> ReadSingleBackwardAsync(
            string partitionId,
            long fromUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort, Limit = 1 };
            ConfigureFindOptions(options);

            using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
            {
                var chunk = await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                return _mongoPayloadSerializer.ApplyDeserialization(chunk);
            }
        }

        public async Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit,
            CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.Gte(x => x.Position, fromPositionInclusive);

            var options = new FindOptions<TChunk>()
            {
                Sort = Builders<TChunk>.Sort.Ascending(x => x.Position)
            };

            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }
            ConfigureFindOptions(options);

            await PushToSubscriber(fromPositionInclusive, subscription, options, filter, true, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.Empty;
            var projection = Builders<TChunk>.Projection.Include(x => x.Position);

            var options = new FindOptions<TChunk, BsonDocument>()
            {
                Sort = Builders<TChunk>.Sort.Descending(x => x.Position),
                Limit = 1,
                Projection = projection
            };
            ConfigureFindOptions(options);

            using (var cursor = await _chunks
                       .FindAsync(filter, options, cancellationToken)
                       .ConfigureAwait(false)
                  )
            {
                var lastPosition = await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
                if (lastPosition != null)
                {
                    return lastPosition[0].AsInt64;
                }

                return 0;
            }
        }

        public async Task<IChunk> ReplaceOneAsync
        (
            long position,
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            var filterByPosition = Builders<TChunk>.Filter.Eq(x => x.Position, position);
            var chunk = new TChunk();
            chunk.Init(
                position,
                partitionId,
                index,
                _mongoPayloadSerializer.Serialize(payload),
                operationId ?? Guid.NewGuid().ToString()
            );

            try
            {
                var result = await _chunks.ReplaceOneAsync(
                    filterByPosition,
                    chunk,
                    (ReplaceOptions)null,
                    cancellationToken).ConfigureAwait(false);

                if (!result.IsAcknowledged)
                {
                    throw new MongoPersistenceException("Replace not Ackowledged");
                }

                return chunk;
            }
            catch (MongoWriteException ex)
            {
                //Need to understand what kind of exception we had, some of them could lead to a retry
                if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                {
                    //Index violation, we do have a chunk that broke an unique index, we need to know if this is 
                    //at partitionId level (concurrency) or at position level (UseLocalSequence == false and multiple process/appdomain are appending to the stream).
#if NET8_0_OR_GREATER
                    var messageSpan = ex.Message.AsSpan();
                    if (messageSpan.Contains(PartitionIndexIdx.AsSpan(), StringComparison.Ordinal))
#else
                    if (ex.Message.Contains(PartitionIndexIdx))
#endif
                    {
                        _logger.LogInformation($"DuplicateStreamIndexException: {ex.Message}.\n{ex.ToString()}");
                        throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                    }

#if NET8_0_OR_GREATER
                    if (messageSpan.Contains(PartitionOperationIdx.AsSpan(), StringComparison.Ordinal))
#else
                    if (ex.Message.Contains(PartitionOperationIdx))
#endif
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                        }

                        return null;
                    }
                }

                throw;
            }
        }

        public async Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
        {
            var filterByPosition = Builders<TChunk>.Filter.Eq(x => x.Position, position);
            var cursor = await _chunks.FindAsync(filterByPosition, cancellationToken: cancellationToken);
            var chunk = await cursor.FirstOrDefaultAsync(cancellationToken: cancellationToken);
            return chunk;
        }

        public async Task DeleteAsync(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            var filterById = Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId);
            if (fromLowerIndexInclusive > 0)
            {
                filterById = Builders<TChunk>.Filter.And(
                    filterById,
                    Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive)
                );
            }

            if (toUpperIndexInclusive < long.MaxValue)
            {
                filterById = Builders<TChunk>.Filter.And(
                    filterById,
                    Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
                );
            }

            await _chunks.DeleteManyAsync(filterById, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId,
            CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Eq(x => x.OperationId, operationId)
            );
            var cursor = await _chunks.FindAsync(filter, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var chunk = await cursor.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            return _mongoPayloadSerializer.ApplyDeserialization(chunk);
        }

        public async Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription,
            CancellationToken cancellationToken)
        {
            var filter = Builders<TChunk>.Filter.Eq(x => x.OperationId, operationId);
            var options = new FindOptions<TChunk>()
            {
                Sort = Builders<TChunk>.Sort.Ascending(x => x.Position)
            };
            ConfigureFindOptions(options);

            await PushToSubscriber(0, subscription, options, filter, true, cancellationToken)
                .ConfigureAwait(false);
        }

        private async Task PersistAsEmptyAsync(
            TChunk chunk,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            TChunk empty;
            //@@REVIEW partial index on mongo?
            if (chunk.PartitionId == "::empty")
            {
                // reuse chunk
                empty = chunk;
                empty.RewriteIndex(empty.Position);
                empty.RewriteOperationId("_" + empty.Position);
            }
            else
            {
                empty = new TChunk();
                empty.Init(
                    chunk.Position,
                    "::empty",
                    chunk.Position,
                    _mongoPayloadSerializer.Serialize(null),
                    "_" + chunk.Position
                );
            }

            await InternalPersistAsync(empty, cancellationToken).ConfigureAwait(false);
        }

        private async Task<IChunk> InternalPersistAsync(
            TChunk chunk,
            CancellationToken cancellationToken = default
        )
        {
            int retry = 0;
            while (true)
            {
                try
                {
                    await _chunks.InsertOneAsync(chunk, cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                    return chunk;
                }
                catch (MongoWriteException ex)
                {
                    //Circuit breaker, if for same reason we cannot write the chunk, we need to stop the process not retrying infiinte times.
                    if (retry++ > 100)
                    {
                        _logger.LogError($"Error During InternalPersistAsync. Reached number of max {retry} retry count: {ex.Message}.\n{ex}");
                        throw;
                    }

                    //Need to understand what kind of exception we had, some of them could lead to a retry
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        //Index violation, we do have a chunk that broke an unique index, we need to know if this is 
                        //at partitionId level (concurrency) or at position level (UseLocalSequence == false and multiple process/appdomain are appending to the stream).
#if NET8_0_OR_GREATER
                        var messageSpan = ex.Message.AsSpan();
                        if (messageSpan.Contains(PartitionIndexIdx.AsSpan(), StringComparison.Ordinal))
#else
                        if (ex.Message.Contains(PartitionIndexIdx))
#endif
                        {
                            // in this situation we have a concurrency exception, to avoid leaving a hole in the position sequence
                            // we persist an empty chunk at this position
                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
                            _logger.LogInformation(
                                $"DuplicateStreamIndexException: {ex.Message}.\n{ex.ToString()}");
                            throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
                        }

#if NET8_0_OR_GREATER
                        if (messageSpan.Contains(PartitionOperationIdx.AsSpan(), StringComparison.Ordinal))
#else
                        if (ex.Message.Contains(PartitionOperationIdx))
#endif
                        {
                            if (cancellationToken.IsCancellationRequested)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                            }

                            //since we should ignore the chunk (already exist a chunk with that operation Id, we fill with a blank).
                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
                            return null;
                        }

#if NET8_0_OR_GREATER
                        if (messageSpan.Contains("_id_".AsSpan(), StringComparison.Ordinal))
#else
                        if (ex.Message.Contains("_id_"))
#endif
                        {
                            if (cancellationToken.IsCancellationRequested)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                            }

                            // some other process steals the Position, we need to warn the user,
                            // because too many of this error could suggest to enable UseLocalSequence
                            // but we can retry because this is not an error at application level
                            // it is only that mongodb have no way to generate a sqlserver identity like field
                            _logger.LogWarning(
                                $@"Error writing chunk #{chunk.Position} - Some other process already wrote position {chunk.Position}. 
Operation will be retried. 
If you see too many of this kind of errors, consider disabling UseLocalSequence because multiple processes are using the very same counter.
Chunk partition {chunk.PartitionId} index {chunk.Index} operationId {chunk.OperationId} chunk payload {chunk.Payload?.GetType().Name}
{ex.Message} - {ex.GetType().FullName} ");
                            await ReloadSequenceAsync(cancellationToken).ConfigureAwait(false);
                            chunk.RewritePosition(await GetNextId(1, cancellationToken).ConfigureAwait(false));
                            continue;
                        }
                    }

                    _logger.LogError($"Error During InternalPersistAsync: {ex.Message}.\n{ex}");
                    throw;
                }
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            if (_options.DropOnInit)
            {
                await DropAsync(cancellationToken).ConfigureAwait(false);
            }

            _chunks = _partitionsDb.GetCollection<TChunk>(_options.PartitionsCollectionName);
            _counters = _countersDb.GetCollection<Counter>(_options.SequenceCollectionName);

            await CreateIndexAsync(cancellationToken).ConfigureAwait(false);

            if (_options.UseLocalSequence)
            {
                await ReloadSequenceAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await EnsureFirstSequenceRecordAsync().ConfigureAwait(false);
            }
        }

        public void Init()
        {
            if (_options.DropOnInit)
            {
                Drop();
            }

            _chunks = _partitionsDb.GetCollection<TChunk>(_options.PartitionsCollectionName);
            _counters = _countersDb.GetCollection<Counter>(_options.SequenceCollectionName);

            CreateIndex();

            if (_options.UseLocalSequence)
            {
                ReloadSequence();
            }
            else
            {
                EnsureFirstSequenceRecord();
            }
        }

        private async Task CreateIndexAsync(CancellationToken cancellationToken)
        {
            //Indexes are created only if connection string is not associated to a standard readonly user.
            if (!_options.ReadonlyUser)
            {
                var partitionIndex = new CreateIndexModel<TChunk>(Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.Index),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = PartitionIndexIdx
                    });

                var partitionOperation = new CreateIndexModel<TChunk>(
                    Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.OperationId),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = PartitionOperationIdx
                    });

                await _chunks.Indexes.CreateOneAsync(
                        partitionIndex,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                await _chunks.Indexes.CreateOneAsync(
                        partitionOperation,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private void CreateIndex()
        {
            if (!_options.ReadonlyUser)
            {
                var partitionIndex = new CreateIndexModel<TChunk>(Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.Index),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = PartitionIndexIdx
                    });

                var partitionOperation = new CreateIndexModel<TChunk>(
                    Builders<TChunk>.IndexKeys
                        .Ascending(x => x.PartitionId)
                        .Ascending(x => x.OperationId),
                    new CreateIndexOptions()
                    {
                        Unique = true,
                        Name = PartitionOperationIdx
                    });

                _chunks.Indexes.CreateOne(partitionIndex);

                _chunks.Indexes.CreateOne(partitionOperation);
            }
        }

        private async Task ReloadSequenceAsync(CancellationToken cancellationToken = default)
        {
            var filter = Builders<TChunk>.Filter.Empty;
            var lastRecord = await _chunks
                .Find(filter)
                .SortByDescending(x => x.Position)
                .Project(Builders<TChunk>.Projection.Include("_id"))
                .Limit(1)
                .ToCursorAsync()
                .ConfigureAwait(false);

            var record = await lastRecord.FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);

            this._sequence = record == null ? 0 : record["_id"].AsInt64;
        }

        private void ReloadSequence()
        {
            var filter = Builders<TChunk>.Filter.Empty;
            var lastRecord = _chunks
                .Find(filter)
                .SortByDescending(x => x.Position)
                .Project(Builders<TChunk>.Projection.Include("_id"))
                .Limit(1)
                .ToCursor();

            var record = lastRecord.FirstOrDefault();

            this._sequence = record == null ? 0 : record["_id"].AsInt64;
        }

        private async Task EnsureFirstSequenceRecordAsync()
        {
            //initialize if needed
            var existing = _counters.AsQueryable().SingleOrDefault(c => c.Id == _options.SequenceId);
            if (existing == null)
            {
                try
                {
                    await _counters.InsertOneAsync(new Counter()
                    {
                        Id = _options.SequenceId,
                        LastValue = 0L
                    }).ConfigureAwait(false);
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        //ignore the error, in the meanwhile between loading existing value and inserting someone else already inserted the record, everything is normal.
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private void EnsureFirstSequenceRecord()
        {
            var existing = _counters.AsQueryable().SingleOrDefault(c => c.Id == _options.SequenceId);
            if (existing == null)
            {
                try
                {
                    _counters.InsertOne(new Counter()
                    {
                        Id = _options.SequenceId,
                        LastValue = 0L
                    });
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
                    {
                        //ignore the error, in the meanwhile between loading existing value and inserting someone else already inserted the record, everything is normal.
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private async ValueTask<long> GetNextId(int size, CancellationToken cancellationToken = default)
        {
            if (_options.UseLocalSequence)
            {
                return Interlocked.Add(ref _sequence, size);
            }

            // server side sequence
            var filter = Builders<Counter>.Filter.Eq(x => x.Id, _options.SequenceId);
            var update = Builders<Counter>.Update.Inc(x => x.LastValue, size);
            var options = new FindOneAndUpdateOptions<Counter>()
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            };

            var updateResult = await _counters.FindOneAndUpdateAsync(
                    filter,
                    update,
                    options,
                    cancellationToken
                )
                .ConfigureAwait(false);

            return updateResult.LastValue;
        }

        public async Task<IChunk> AppendAsync(
            string partitionId,
            long index,
            object payload,
            string operationId,
            CancellationToken cancellationToken)
        {
            if (index < 0)
            {
                throw new InvalidStreamIndexException(partitionId, index);
            }

            long id = await GetNextId(1, cancellationToken).ConfigureAwait(false);
            var chunk = new TChunk();
            chunk.Init(
                id,
                partitionId,
                index,
                _mongoPayloadSerializer.Serialize(payload),
                operationId ?? Guid.NewGuid().ToString()
            );
            return await InternalPersistAsync(chunk, cancellationToken).ConfigureAwait(false);
        }

        public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
        {
            int retry = 0;
            var insertCount = queue.Length;
            var lastId = await GetNextId(insertCount, cancellationToken)
                .ConfigureAwait(false);

            var firstId = lastId - insertCount + 1;
            List<WriteJob> writeJobs = CreateWriteJobs(queue, insertCount, firstId);

            var options = new InsertManyOptions()
            {
                IsOrdered = false,
            };

            // Simple retry loop: continue while there are jobs to save and retry count is less than max retries
            while (writeJobs.Count > 0 && retry < _options.BatchAppendMaxRetries)
            {
                try
                {
                    await _chunks
                        .InsertManyAsync(writeJobs.Select(j => (TChunk)j.Chunk).ToArray(), options, cancellationToken)
                        .ConfigureAwait(false);

                    // All succeeded, mark all jobs as committed
                    foreach (var job in writeJobs)
                    {
                        job.Succeeded();
                    }

                    // Clear the list - all jobs succeeded we can exit.
                    writeJobs.Clear();
                }
                catch (MongoBulkWriteException<TChunk> e)
                {
                    retry++;
                    var hasRecoverableError = false;
                    var jobsToRetry = new List<WriteJob>();
                    var jobsToRemove = new HashSet<WriteJob>();

                    foreach (var err in e.WriteErrors)
                    {
                        var failedJob = writeJobs[err.Index];
                        if (err.Category == ServerErrorCategory.DuplicateKey)
                        {
#if NET8_0_OR_GREATER
                            var errorMessageSpan = err.Message.AsSpan();
                            if (errorMessageSpan.Contains(PartitionIndexIdx.AsSpan(), StringComparison.Ordinal))
#else
                            if (err.Message.Contains(PartitionIndexIdx))
#endif
                            {
                                // Non-recoverable: duplicated index
                                failedJob.Failed(WriteJob.WriteResult.DuplicatedIndex);
                                jobsToRemove.Add(failedJob);
                                continue;
                            }

#if NET8_0_OR_GREATER
                            if (errorMessageSpan.Contains(PartitionOperationIdx.AsSpan(), StringComparison.Ordinal))
#else
                            if (err.Message.Contains(PartitionOperationIdx))
#endif
                            {
                                // Non-recoverable: duplicated operation
                                failedJob.Failed(WriteJob.WriteResult.DuplicatedOperation);
                                jobsToRemove.Add(failedJob);
                                continue;
                            }

#if NET8_0_OR_GREATER
                            if (errorMessageSpan.Contains("_id_".AsSpan(), StringComparison.Ordinal))
#else
                            if (err.Message.Contains("_id_"))
#endif
                            {
                                // Recoverable: duplicated position - can retry with new position
                                failedJob.Failed(WriteJob.WriteResult.DuplicatedPosition);
                                jobsToRetry.Add(failedJob);
                                hasRecoverableError = true;
                                continue;
                            }
                        }

                        // Any other error type marks the job as failed (non-recoverable)
                        failedJob.Failed(WriteJob.WriteResult.Failed);
                        jobsToRemove.Add(failedJob);
                    }

                    // Mark successfully written chunks
                    for (var i = 0; i < writeJobs.Count; i++)
                    {
                        var job = writeJobs[i];
                        // If the job is not in failed or retry lists, it succeeded
                        if (!jobsToRemove.Contains(job) && !jobsToRetry.Contains(job))
                        {
                            job.Succeeded();
                            jobsToRemove.Add(job);
                        }
                    }

                    // Remove all succeeded and non-recoverable failed jobs from the list
                    foreach (var job in jobsToRemove)
                    {
                        writeJobs.Remove(job);
                    }

                    if (hasRecoverableError && jobsToRetry.Count > 0)
                    {
                        await PrepareNewLoopData(writeJobs, jobsToRetry, cancellationToken).ConfigureAwait(false);
                    }
                }
            }

            // Check if we exited due to retry limit
            if (retry >= _options.BatchAppendMaxRetries)
            {
                // Mark remaining jobs as failed with DuplicatedPosition before throwing
                foreach (var job in writeJobs)
                {
                    if (job.Result == WriteJob.WriteResult.None || job.Result == WriteJob.WriteResult.DuplicatedPosition)
                    {
                        job.Failed(WriteJob.WriteResult.DuplicatedPosition);
                    }
                }

                var failedJobsCount = writeJobs.Count;
                _logger.LogError($"Error During AppendBatchAsync. Reached maximum retry limit of {_options.BatchAppendMaxRetries}. Failed jobs: {failedJobsCount}");
                throw new BatchRetryLimitExceededException(retry, _options.BatchAppendMaxRetries, failedJobsCount);
            }
        }

        private async Task PrepareNewLoopData(List<WriteJob> writeJobs, List<WriteJob> jobsToRetry, CancellationToken cancellationToken)
        {
            // Reload sequence and prepare retry for failed chunks only
            _logger.LogWarning($@"Error writing batch - Some positions were already taken.
Operation will be retried for failed chunks only.
If you see too many of this kind of errors, consider disabling UseLocalSequence because multiple processes are using the very same counter.");

            await ReloadSequenceAsync(cancellationToken).ConfigureAwait(false);

            // Get new positions for retry chunks
            var newLastId = await GetNextId(jobsToRetry.Count, cancellationToken).ConfigureAwait(false);
            var newFirstId = newLastId - jobsToRetry.Count + 1;

            // Create new chunks with new positions for retry jobs
            for (var i = 0; i < jobsToRetry.Count; i++)
            {
                var job = jobsToRetry[i];

                var newChunk = new TChunk();
                newChunk.Init(
                    newFirstId + i,
                    job.PartitionId,
                    job.Index,
                    _mongoPayloadSerializer.Serialize(job.Payload),
                    job.OperationId
                );

                // Set new chunk and reset result to None for retry
                job.SetChunk(newChunk);
                job.Failed(WriteJob.WriteResult.None);
            }

            // Remove retry jobs from main list (they were marked as failed)
            // and rebuild the writeJobs list with only retry jobs
            writeJobs.Clear();
            writeJobs.AddRange(jobsToRetry);
        }

        private List<WriteJob> CreateWriteJobs(WriteJob[] queue, int insertCount, long firstId)
        {
            // Create write jobs list and set chunks before saving
            var writeJobs = new List<WriteJob>(insertCount);
            for (var currentIdx = 0; currentIdx < insertCount; currentIdx++)
            {
                var job = queue[currentIdx];
                long id = firstId + currentIdx;

                var chunk = new TChunk();
                chunk.Init(
                    id,
                    job.PartitionId,
                    job.Index,
                    _mongoPayloadSerializer.Serialize(job.Payload),
                    job.OperationId ?? Guid.NewGuid().ToString()
                );

                // Set chunk before saving
                job.SetChunk(chunk);
                writeJobs.Add(job);
            }

            return writeJobs;
        }

        #region IPartitionPersistenceSync

        public IReadOnlyList<IChunk> ReadForward(
            string partitionId,
            long fromLowerIndexInclusive,
            long toUpperIndexInclusive,
            int limit
        )
        {
            if (limit <= 0)
            {
                return Array.Empty<IChunk>();
            }

            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Ascending(x => x.Index);
            var findFluent = _chunks.Find(filter).Sort(sort);
            if (limit != int.MaxValue)
            {
                findFluent = findFluent.Limit(limit);
            }

            var chunks = findFluent.ToList();
            foreach (var chunk in chunks)
            {
                _mongoPayloadSerializer.ApplyDeserialization(chunk);
            }

            return chunks.Cast<IChunk>().ToList();
        }

        public IReadOnlyList<IChunk> ReadBackward(
            string partitionId,
            long fromUpperIndexInclusive,
            long toLowerIndexInclusive,
            int limit
        )
        {
            if (limit <= 0)
            {
                return Array.Empty<IChunk>();
            }

            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive),
                Builders<TChunk>.Filter.Gte(x => x.Index, toLowerIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var findFluent = _chunks.Find(filter).Sort(sort);
            if (limit != int.MaxValue)
            {
                findFluent = findFluent.Limit(limit);
            }

            var chunks = findFluent.ToList();
            foreach (var chunk in chunks)
            {
                _mongoPayloadSerializer.ApplyDeserialization(chunk);
            }

            return chunks.Cast<IChunk>().ToList();
        }

        public IChunk ReadSingleBackward(
            string partitionId,
            long fromUpperIndexInclusive
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Lte(x => x.Index, fromUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Descending(x => x.Index);
            var chunk = _chunks.Find(filter).Sort(sort).Limit(1).FirstOrDefault();
            return _mongoPayloadSerializer.ApplyDeserialization(chunk);
        }

        public IChunk ReadByOperationId(
            string partitionId,
            string operationId
        )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Eq(x => x.OperationId, operationId)
            );

            var chunk = _chunks.Find(filter).FirstOrDefault();
            return _mongoPayloadSerializer.ApplyDeserialization(chunk);
        }

        #endregion
    }
}
