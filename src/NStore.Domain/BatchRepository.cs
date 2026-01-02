using System;
using System.Collections.Generic;
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

        private readonly IDictionary<string, IAggregate> _trackingAggregates = new Dictionary<string, IAggregate>();
        private readonly IDictionary<string, long> _aggregateVersions = new Dictionary<string, long>();

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

        public async Task<IDictionary<string, T>> GetManyByIdAsync<T>(
            IEnumerable<string> ids,
            CancellationToken cancellationToken = default) where T : IAggregate
        {
            var requestsList = ids.ToList();
            if (!requestsList.Any())
            {
                return new Dictionary<string, T>();
            }

            var result = new Dictionary<string, T>();

            // Step 1: Check tracking cache first
            var idsToLoad = new List<string>();
            foreach (var id in requestsList)
            {
                if (_trackingAggregates.TryGetValue(id, out var cachedAggregate))
                {
                    result[id] = (T)cachedAggregate;
                }
                else
                {
                    idsToLoad.Add(id);
                }
            }

            if (!idsToLoad.Any())
            {
                return result;
            }

            // Step 2: Create aggregates
            var aggregates = new Dictionary<string, T>();
            foreach (var id in idsToLoad)
            {
                var aggregate = _factory.Create<T>();
                aggregates[id] = aggregate;
                _trackingAggregates[id] = aggregate;
            }

            // Step 3: Load snapshots if available
            if (_snapshotBatchStore != null)
            {
                var snapshotIds = idsToLoad.ToList();
                var snapshots = await _snapshotBatchStore.GetManyAsync(snapshotIds, cancellationToken).ConfigureAwait(false);

                foreach (var kvp in snapshots)
                {
                    if (aggregates.TryGetValue(kvp.Key, out var aggregate) && aggregate is ISnapshottable snapshottable)
                    {
                        snapshottable.TryRestore(kvp.Value);
                    }
                }
            }

            // Step 3b: Initialize aggregates that weren't restored from snapshots
            foreach (var kvp in aggregates)
            {
                if (!kvp.Value.IsInitialized)
                {
                    kvp.Value.Init(kvp.Key);
                }
            }

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
            var eventsByPartition = new Dictionary<string, List<IChunk>>();
            var subscription = new LambdaSubscription(chunk =>
            {
                if (!eventsByPartition.TryGetValue(chunk.PartitionId, out var chunks))
                {
                    chunks = new List<IChunk>();
                    eventsByPartition[chunk.PartitionId] = chunks;
                }
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

            // Step 6: Apply events to aggregates
            foreach (var kvp in aggregates)
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
                    throw new StaleSnapshotException(kvp.Key, snapshotVersion);
                }

                // Track the current version for optimistic concurrency
                _aggregateVersions[kvp.Key] = kvp.Value.Version;

                result[kvp.Key] = kvp.Value;
            }

            return result;
        }

        public async Task SaveManyAsync(
            IEnumerable<IAggregate> aggregates,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default)
        {
            var aggregatesList = aggregates.ToList();
            if (!aggregatesList.Any())
            {
                return;
            }

            // Step 1: Validate all aggregates and prepare write jobs
            var writeJobs = new List<WriteJob>();
            var aggregateByPartitionId = new Dictionary<string, IAggregate>();

            foreach (var aggregate in aggregatesList)
            {
                // Validate aggregate is tracked by this repository (unless it's a new aggregate)
                if (!_trackingAggregates.Values.Contains(aggregate) && !aggregate.IsNew)
                {
                    throw new RepositoryMismatchException($"Aggregate {aggregate.Id} was not loaded by this batch repository");
                }

                var persister = (IEventSourcedAggregate)aggregate;
                var changeSet = persister.GetChangeSet();

                // Skip empty changesets unless configured to persist them
                if (changeSet.IsEmpty() && !PersistEmptyChangeset)
                {
                    continue;
                }

                // Check invariants if supported
                if (aggregate is IInvariantsChecker checker)
                {
                    var check = checker.CheckInvariants();
                    check.ThrowIfInvalid();
                }

                // Apply headers if provided
                headers?.Invoke(changeSet);

                // Determine the current version for optimistic concurrency control
                long currentVersion;
                if (aggregate.IsNew)
                {
                    // New aggregates start at version 0, will write at index 1
                    currentVersion = 0;
                    _aggregateVersions[aggregate.Id] = 0;
                    _trackingAggregates[aggregate.Id] = aggregate;
                }
                else if (_aggregateVersions.TryGetValue(aggregate.Id, out var trackedVersion))
                {
                    // Use the tracked version from when we loaded the aggregate
                    currentVersion = trackedVersion;
                }
                else
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

            if (!writeJobs.Any())
            {
                return;
            }

            // Step 2: Execute batch append
            await _persistence.AppendBatchAsync(writeJobs.ToArray(), cancellationToken).ConfigureAwait(false);

            // Step 3: Process results and update aggregate states
            var failedAggregates = new List<AggregateFailureInfo>();
            var succeededIds = new List<string>();
            var snapshotsToSave = new Dictionary<string, SnapshotInfo>();

            foreach (var job in writeJobs)
            {
                var aggregate = aggregateByPartitionId[job.PartitionId];
                var persister = (IEventSourcedAggregate)aggregate;

                switch (job.Result)
                {
                    case WriteJob.WriteResult.Committed:
                        // Success - update tracked version and notify aggregate
                        _aggregateVersions[aggregate.Id] = job.Index;
                        persister.Persisted(persister.GetChangeSet());
                        succeededIds.Add(aggregate.Id);

                        // Collect snapshot if supported
                        if (_snapshotBatchStore != null && aggregate is ISnapshottable snapshottable)
                        {
                            snapshotsToSave[aggregate.Id] = snapshottable.GetSnapshot();
                        }
                        break;

                    case WriteJob.WriteResult.DuplicatedIndex:
                        // Optimistic concurrency violation - another process modified this aggregate
                        failedAggregates.Add(new AggregateFailureInfo(
                            aggregate.Id,
                            aggregate.GetType(),
                            AggregateFailureReason.ConcurrencyConflict
                        ));
                        break;

                    case WriteJob.WriteResult.DuplicatedOperation:
                        // Operation was already executed (idempotency check passed) - treat as success
                        succeededIds.Add(aggregate.Id);
                        break;

                    case WriteJob.WriteResult.Failed:
                        // Generic failure from persistence layer
                        failedAggregates.Add(new AggregateFailureInfo(
                            aggregate.Id,
                            aggregate.GetType(),
                            AggregateFailureReason.Failed
                        ));
                        break;

                    case WriteJob.WriteResult.DuplicatedPosition:
                        // Position conflict after retry limit exceeded
                        failedAggregates.Add(new AggregateFailureInfo(
                            aggregate.Id,
                            aggregate.GetType(),
                            AggregateFailureReason.Failed
                        ));
                        break;
                }
            }

            // Step 4: Save snapshots in batch (best-effort)
            if (snapshotsToSave.Count > 0)
            {
                await _snapshotBatchStore.AddManyAsync(snapshotsToSave, cancellationToken).ConfigureAwait(false);
            }

            // Step 5: Throw exception if there were any failures
            if (failedAggregates.Any())
            {
                throw new BatchConcurrencyException(failedAggregates, succeededIds);
            }
        }

        public void Clear()
        {
            _trackingAggregates.Clear();
            _aggregateVersions.Clear();
        }
    }
}
