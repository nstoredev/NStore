using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.SnapshotStore
{
    public class DefaultSnapshotStore : ISnapshotStore
    {
        private readonly IPersistence _store;

        public DefaultSnapshotStore(IPersistence store)
        {
            _store = store;
        }

        public async Task<SnapshotInfo> Get(
            string aggregateId,
            int version,
            CancellationToken cancellationToken)
        {
            var data = await _store.ReadSingleBackwardAsync(aggregateId, version, cancellationToken).ConfigureAwait(false);
            return (SnapshotInfo) data?.Payload;
        }

        public async Task Add(
            string aggregateId,
            SnapshotInfo snapshot,
            CancellationToken cancellationToken)
        {
            if (snapshot == null || snapshot.IsEmpty)
                return;

            try
            {
                await _store.AppendAsync(aggregateId, snapshot.AggregateVersion, snapshot, null, cancellationToken).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException)
            {
                // already stored
            }
        }

        public Task Remove(
            string aggregateId,
            int fromVersionInclusive,
            int toVersionInclusive,
            CancellationToken cancellationToken)
        {
            return _store.DeleteAsync(aggregateId, fromVersionInclusive, toVersionInclusive, cancellationToken);
        }
    }
}
