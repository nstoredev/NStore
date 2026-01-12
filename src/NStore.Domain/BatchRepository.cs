using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;

namespace NStore.Domain
{
    /// <summary>
    /// Batch repository implementation for efficient multi-aggregate operations.
    /// Uses ISnapshotBatchStore, IMultiPartitionPersistenceReader, and IEnhancedPersistence
    /// to perform batch reads and writes with optimistic concurrency control.
    /// </summary>
    public class BatchRepository : IBatchRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IMultiPartitionPersistenceReader _multiReader;
        private readonly IEnhancedPersistence _persistence;
        private readonly ISnapshotBatchStore _snapshotBatchStore;

        private readonly Dictionary<string, IAggregate> _trackingAggregates = new Dictionary<string, IAggregate>();

        public bool PersistEmptyChangeset { get; set; } = false;

        public BatchRepository(
            IAggregateFactory factory,
            IMultiPartitionPersistenceReader multiReader,
            IEnhancedPersistence persistence,
            ISnapshotBatchStore snapshotStore)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _multiReader = multiReader ?? throw new ArgumentNullException(nameof(multiReader));
            _persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
            _snapshotBatchStore = snapshotStore;
        }

        public async Task<IReadOnlyDictionary<string, T>> GetManyByIdAsync<T>(
            IEnumerable<string> ids,
            CancellationToken cancellationToken = default) where T : IAggregate, IEventSourcedAggregate
        {
            if (!ids.Any())
            {
#if NET8_0_OR_GREATER
                return System.Collections.ObjectModel.ReadOnlyDictionary<string, T>.Empty;
#else
                return new ReadOnlyDictionary<string, T>(new Dictionary<string, T>());
#endif
            }

            var aggregates = new Dictionary<string, T>();

            // Step 1: Check tracking cache first
            List<string> idsToLoad = GetListOfIdToLoad(ids, aggregates);

            // all id were already loaded. we can exit.
            if (idsToLoad.Count == 0)
            {
                return aggregates;
            }

            // Step 2: Create aggregates to be loaded with event sourcing.
            CreateAggregates(aggregates, idsToLoad);

            // Step 3: Load snapshots if available
            await LoadSnapshots(aggregates, idsToLoad, cancellationToken).ConfigureAwait(false);

            // Step 3b: Initialize aggregates that weren't restored from snapshots
            foreach (var notInitializedAggregateEntry in aggregates.Where(a => !a.Value.IsInitialized))
            {
                notInitializedAggregateEntry.Value.Init(notInitializedAggregateEntry.Key);
            }

            // Step 4: Build partition read requests based on aggregate versions
            var partitionRequests = new List<PartitionReadRequest>();
            var snapshotVersions = new Dictionary<string, long>();
            LoadPartitionRequest(aggregates, partitionRequests, snapshotVersions);

            // Step 5: Read events from multiple partitions, it is imperative that we do not store
            //         chunks in memory or we will use too much memory for large batches.
            var countOfEventsRead = new Dictionary<string, int>();
            var subscription = new LambdaSubscription(chunk =>
            {
                //we immediately restore the aggregate
                var partitionId = chunk.PartitionId;
                if (aggregates.TryGetValue(partitionId, out var aggregate))
                {
                    aggregate.ApplyChanges((Changeset)chunk.Payload);
                    if (!countOfEventsRead.ContainsKey(partitionId))
                    {
                        countOfEventsRead[partitionId] = 0;
                    }
                    countOfEventsRead[partitionId] += 1;
                }

                // the else should never happen, but just in case, we ignore chunks for unknown partitions
                return Task.FromResult(true);
            });

            await _multiReader.ReadForwardMultiplePartitionsWithRangesAsync(
                partitionRequests,
                subscription,
                cancellationToken
            ).ConfigureAwait(false);

            // Check for subscription errors
            if (subscription.Failed)
            {
                var aggregateIds = string.Join(", ", aggregates.Keys);
                throw new RepositoryReadException(
                    $"Error reading aggregates in batch. Aggregate IDs: [{aggregateIds}]",
                    subscription.LastError);
            }

            // now we need to understand if we have stale snapshot and mark all the aggregates as initialized
            foreach (var kvp in aggregates)
            {
                var aggregate = kvp.Value;
                aggregate.Loaded();
                var snapshotVersion = snapshotVersions[kvp.Key];
                if (!countOfEventsRead.TryGetValue(kvp.Key, out var eventsRead))
                {
                    eventsRead = 0;
                }
                if (snapshotVersion > 0 && eventsRead == 0)
                {
                    // immediately throw
                    throw new StaleSnapshotException(aggregate.Id, snapshotVersion);
                }
            }

            return aggregates;
        }

        private static void LoadPartitionRequest<T>(Dictionary<string, T> aggregates, List<PartitionReadRequest> partitionRequests, Dictionary<string, long> snapshotVersions) where T : IAggregate
        {
            foreach (var kvp in aggregates)
            {
                // Track snapshot version for stale snapshot detection
                snapshotVersions[kvp.Key] = kvp.Value.Version;

                // Read from current version to end
                // Note: Starting point is inclusive, so we read from aggregate.Version
                // The aggregate will ignore duplicate events because ApplyChanges is idempotent
                partitionRequests.Add(new PartitionReadRequest(
                    kvp.Key,
                    kvp.Value.Version,
                    long.MaxValue
                ));
            }
        }

        private async Task LoadSnapshots<T>(Dictionary<string, T> aggregates, List<string> idsToLoad, CancellationToken cancellationToken) where T : IAggregate
        {
            //TODO load snapshot only for the aggregate id that support snapshotting
            if (_snapshotBatchStore != null)
            {
                var snapshots = await _snapshotBatchStore.GetManyAsync(idsToLoad, cancellationToken).ConfigureAwait(false);

                Parallel.ForEach(snapshots, kvp =>
                {
                    if (aggregates.TryGetValue(kvp.Key, out var aggregate) && aggregate is ISnapshottable snapshottable)
                    {
                        snapshottable.TryRestore(kvp.Value);
                    }
                });
            }
        }

        private void CreateAggregates<T>(Dictionary<string, T> aggregates, List<string> idsToLoad) where T : IAggregate
        {
            foreach (var id in idsToLoad)
            {
                var aggregate = _factory.Create<T>();
                aggregates[id] = aggregate;
                _trackingAggregates[id] = aggregate;
            }
        }

        private List<string> GetListOfIdToLoad<T>(IEnumerable<string> ids, Dictionary<string, T> aggregates) where T : IAggregate
        {
            var idsToLoad = new List<string>();
            foreach (var id in ids)
            {
                if (_trackingAggregates.TryGetValue(id, out var cachedAggregate))
                {
                    aggregates[id] = (T)cachedAggregate;
                }
                else
                {
                    idsToLoad.Add(id);
                }
            }

            return idsToLoad;
        }

        public async Task<BatchSaveResult> SaveManyAsync(
            IEnumerable<IAggregate> aggregates,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default)
        {
            if (!aggregates.Any())
            {
                return BatchSaveResult.Empty;
            }

            // Step 1: Validate all aggregates and prepare write jobs
            var writeJobs = new List<WriteJob>();
            var aggregateByPartitionId = new Dictionary<string, IAggregate>();

            // first step is preparing the write jobs for all aggregates to persist in a batch.
            var seen = new HashSet<string>();
            var listOfAggregateSaveResult = new List<AggregateSaveResult>();
            foreach (var aggregate in aggregates)
            {
                // Check for duplicate aggregate ids in the input - this is not allowed
                if (!seen.Add(aggregate.Id))
                {
                    throw new ArgumentException($"Duplicate aggregate id '{aggregate.Id}' passed to SaveManyAsync");
                }

                // Validate aggregate is tracked by this repository (unless it's a new aggregate)
                if (!_trackingAggregates.ContainsKey(aggregate.Id))
                {
                    if (aggregate.IsNew)
                    {
                        _trackingAggregates[aggregate.Id] = aggregate;
                    }
                    else
                    {
                        throw new RepositoryMismatchException($"Aggregate {aggregate.Id} was not loaded by this batch repository");
                    }
                }

                var persister = (IEventSourcedAggregate)aggregate;
                var changeSet = persister.GetChangeSet();

                // Skip empty changesets unless configured to persist them; report as Unchanged instead of omitting
                if (changeSet.IsEmpty() && !PersistEmptyChangeset)
                {
                    listOfAggregateSaveResult.Add(AggregateSaveResult.Unchanged(aggregate.Id));
                    continue;
                }

                // Check invariants if supported - collect failures to report per-aggregate instead of throwing
                if (aggregate is IInvariantsChecker checker)
                {
                    var check = checker.CheckInvariants();
                    if (check.IsInvalid)
                    {
                        listOfAggregateSaveResult.Add(AggregateSaveResult.InvariantFailure(aggregate.Id));
                        continue;
                    }
                }

                // Apply headers if provided
                headers?.Invoke(changeSet);

                // Create write job - use changeSet.AggregateVersion which is already Version + 1
                var job = new WriteJob(
                    aggregate.Id,
                    changeSet.AggregateVersion,
                    changeSet,
                    operationId ?? Guid.NewGuid().ToString()
                );

                writeJobs.Add(job);
                aggregateByPartitionId[aggregate.Id] = aggregate;
            }

            if (writeJobs.Count > 0)
            {
                // Step 2: Execute batch append
                await _persistence.AppendBatchAsync(writeJobs.ToArray(), cancellationToken).ConfigureAwait(false);
                var snapshotsToSave = new Dictionary<string, SnapshotInfo>();

                foreach (var job in writeJobs)
                {
                    var aggregate = aggregateByPartitionId[job.PartitionId];
                    listOfAggregateSaveResult.Add(MapJobResult(job, aggregate.Id));

                    if (job.Result == WriteJob.WriteResult.Committed)
                    {
                        ((IEventSourcedAggregate)aggregate).Persisted(((IEventSourcedAggregate)aggregate).GetChangeSet());
                        if (_snapshotBatchStore != null && aggregate is ISnapshottable snapshottable)
                        {
                            snapshotsToSave[aggregate.Id] = snapshottable.GetSnapshot();
                        }
                    }
                }

                // Step 4: Save snapshots in batch (best-effort)
                if (_snapshotBatchStore != null && snapshotsToSave.Count > 0)
                {
                    await _snapshotBatchStore.AddManyAsync(snapshotsToSave, cancellationToken).ConfigureAwait(false);
                }
            }

            // Step 5: Return batch save result with all aggregate results
            return new BatchSaveResult
            {
                Results = listOfAggregateSaveResult
            };
        }

        public void Clear()
        {
            _trackingAggregates.Clear();
        }

        private static AggregateSaveResult MapJobResult(WriteJob job, string aggregateId)
        {
            switch (job.Result)
            {
                case WriteJob.WriteResult.Committed: return AggregateSaveResult.Committed(aggregateId, job.Chunk);
                case WriteJob.WriteResult.DuplicatedIndex: return AggregateSaveResult.Concurrency(aggregateId);
                case WriteJob.WriteResult.DuplicatedOperation: return AggregateSaveResult.DuplicatedOperation(aggregateId, job.Chunk);
                case WriteJob.WriteResult.DuplicatedPosition: return AggregateSaveResult.DuplicatedPosition(aggregateId);
                
                case WriteJob.WriteResult.None:
                case WriteJob.WriteResult.Failed:
                default: return AggregateSaveResult.GenericFailure(aggregateId);
            }
        }
    }
}
