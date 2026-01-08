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

        private readonly ConcurrentDictionary<string, IAggregate> _trackingAggregates = new ConcurrentDictionary<string, IAggregate>();
        private readonly ConcurrentDictionary<string, long> _aggregateVersions = new ConcurrentDictionary<string, long>();

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
            CancellationToken cancellationToken = default) where T : IAggregate
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

            // all id were already loaded. we can exit.
            if (idsToLoad.Count == 0)
            {
                return aggregates;
            }

            // Step 2: Load aggregate with event sourcing.
            foreach (var id in idsToLoad)
            {
                var aggregate = _factory.Create<T>();
                aggregates[id] = aggregate;
                _trackingAggregates[id] = aggregate;
            }

            // Step 3: Load snapshots if available
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

            // Step 3b: Initialize aggregates that weren't restored from snapshots
            Parallel.ForEach(aggregates, kvp =>
            {
                if (!kvp.Value.IsInitialized)
                {
                    kvp.Value.Init(kvp.Key);
                }
            });

            // Step 4: Build partition read requests based on aggregate versions
            var partitionRequests = new List<PartitionReadRequest>();
            var snapshotVersions = new Dictionary<string, long>();
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

            // Step 5: Read events from multiple partitions
            var eventsByPartition = new ConcurrentDictionary<string, ConcurrentBag<IChunk>>();
            var subscription = new LambdaSubscription(chunk =>
            {
                var chunks = eventsByPartition.GetOrAdd(chunk.PartitionId, _ => new ConcurrentBag<IChunk>());
                chunks.Add(chunk);
                return Task.FromResult(true);
            });

            await _multiReader.ReadForwardMultiplePartitionsWithRangesAsync(
                partitionRequests,
                subscription,
                cancellationToken
            ).ConfigureAwait(false);

            if (subscription.Failed)
            {
                var aggregateIds = string.Join(", ", aggregates.Keys);
                throw new RepositoryReadException(
                    $"Error reading aggregates in batch. Aggregate IDs: [{aggregateIds}]",
                    subscription.LastError);
            }

            // Step 6: Apply events to aggregates (in parallel for performance)
            var exceptions = new ConcurrentBag<Exception>();

            Parallel.ForEach(aggregates, kvp =>
            {
                try
                {
                    var persister = (IEventSourcedAggregate)kvp.Value;
                    int eventsRead = 0;

                    if (eventsByPartition.TryGetValue(kvp.Key, out var chunks))
                    {
                        foreach (var chunk in chunks.OrderBy(c => c.Index))
                        {
                            eventsRead++;
                            persister.ApplyChanges((Changeset)chunk.Payload);
                        }
                    }

                    persister.Loaded();

                    // Validate snapshot: if we had a snapshot but read no events, the snapshot is stale
                    var snapshotVersion = snapshotVersions[kvp.Key];
                    if (snapshotVersion > 0 && eventsRead == 0)
                    {
                        exceptions.Add(new StaleSnapshotException(kvp.Key, snapshotVersion));
                        return;
                    }

                    // Track the current version for optimistic concurrency
                    _aggregateVersions[kvp.Key] = kvp.Value.Version;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            // Throw the first exception if any occurred during parallel processing
            if (!exceptions.IsEmpty)
            {
                throw exceptions.First();
            }

            return aggregates;
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

            // Check for duplicate aggregate ids in the input - this is not allowed
            var duplicate = aggregates.GroupBy(a => a.Id).FirstOrDefault(g => g.Count() > 1);
            if (duplicate != null)
            {
                throw new ArgumentException($"Duplicate aggregate id '{duplicate.Key}' passed to SaveManyAsync");
            }

            // first step is preparing the write jobs for all aggregates to persist in a batch.
            var listOfAggregateSaveResult = new List<AggregateSaveResult>();
            foreach (var aggregate in aggregates)
            {
                // Validate aggregate is tracked by this repository (unless it's a new aggregate)
                if (!_trackingAggregates.Values.Contains(aggregate))
                {
                    if (aggregate.IsNew)
                    {
                        _trackingAggregates[aggregate.Id] = aggregate;
                        _aggregateVersions[aggregate.Id] = 0;
                    }
                    else
                    {
                        //this operation is not permitted something bad happens.
                        throw new RepositoryMismatchException($"Aggregate {aggregate.Id} was not loaded by this batch repository");
                    }
                }

                var persister = (IEventSourcedAggregate)aggregate;
                var changeSet = persister.GetChangeSet();

                // Skip empty changesets unless configured to persist them; report as Unchanged instead of omitting
                if (changeSet.IsEmpty() && !PersistEmptyChangeset)
                {
                    //we do not need to save the aggregate, just report it as unchanged
                    listOfAggregateSaveResult.Add(new AggregateSaveResult
                    {
                        AggregateId = aggregate.Id,
                        Succeeded = true,
                        Chunk = null,
                        FailureKind = AggregateSaveFailureKind.Unchanged
                    });
                    continue;
                }

                // Check invariants if supported - collect failures to report per-aggregate instead of throwing
                if (aggregate is IInvariantsChecker checker)
                {
                    var check = checker.CheckInvariants();
                    if (check.IsInvalid)
                    {
                        listOfAggregateSaveResult.Add(new AggregateSaveResult
                        {
                            AggregateId = aggregate.Id,
                            Succeeded = false,
                            Chunk = null,
                            FailureKind = AggregateSaveFailureKind.InvariantFailure
                        });
                        continue;
                    }
                }

                // Apply headers if provided
                headers?.Invoke(changeSet);

                // Determine the current version for optimistic concurrency control 
                if (!_aggregateVersions.TryGetValue(aggregate.Id, out var currentVersion))
                {
                    throw new RepositoryMismatchException($"Aggregate {aggregate.Id} version not tracked");
                }
               
                // Calculate the next version index to write
                long desiredVersion = currentVersion + 1;

                // Create write job
                var job = new WriteJob(
                    aggregate.Id,
                    desiredVersion,
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
                    var persister = (IEventSourcedAggregate)aggregate;
                    var saveResult = new AggregateSaveResult
                    {
                        AggregateId = aggregate.Id
                    };

                    switch (job.Result)
                    {
                        case WriteJob.WriteResult.Committed:
                            // Success - update tracked version and notify aggregate
                            _aggregateVersions[aggregate.Id] = job.Index;
                            persister.Persisted(persister.GetChangeSet());

                            saveResult.Succeeded = true;
                            saveResult.Chunk = job.Chunk;
                            saveResult.FailureKind = null;

                            // Collect snapshot if supported
                            if (_snapshotBatchStore != null && aggregate is ISnapshottable snapshottable)
                            {
                                snapshotsToSave[aggregate.Id] = snapshottable.GetSnapshot();
                            }
                            break;

                        case WriteJob.WriteResult.DuplicatedIndex:
                            // Optimistic concurrency violation - another process modified this aggregate
                            saveResult.Succeeded = false;
                            saveResult.Chunk = null;
                            saveResult.FailureKind = AggregateSaveFailureKind.Concurrency;
                            break;

                        case WriteJob.WriteResult.DuplicatedOperation:
                            // Operation was already executed (idempotency check passed) - treat as success
                            saveResult.Succeeded = true;
                            saveResult.Chunk = job.Chunk; // May be null for duplicated operations
                            saveResult.FailureKind = AggregateSaveFailureKind.DuplicatedOperation;
                            break;

                        case WriteJob.WriteResult.Failed:
                            // Generic failure from persistence layer
                            saveResult.Succeeded = false;
                            saveResult.Chunk = null;
                            saveResult.FailureKind = AggregateSaveFailureKind.GenericFailure;
                            break;

                        case WriteJob.WriteResult.DuplicatedPosition:
                            // Position conflict after retry limit exceeded
                            saveResult.Succeeded = false;
                            saveResult.Chunk = null;
                            saveResult.FailureKind = AggregateSaveFailureKind.DuplicatedPosition;
                            break;
                    }

                    listOfAggregateSaveResult.Add(saveResult);
                }

                // Step 4: Save snapshots in batch (best-effort)
                if (snapshotsToSave.Count > 0)
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
            _aggregateVersions.Clear();
        }
    }
}
