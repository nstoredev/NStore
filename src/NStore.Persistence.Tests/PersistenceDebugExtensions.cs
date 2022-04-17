using NStore.Core.Persistence;
using System;
using System.Threading.Tasks;

namespace NStore.Persistence.Tests
{
    public static class PersistenceDebugExtensions
    {
        public static async Task<AllPartitionsRecorder> GetAllEventForAPartition(this IStore store, String partitionId)
        {
            var tape = new AllPartitionsRecorder();
            await store.ReadForwardAsync(partitionId, tape).ConfigureAwait(false);
            return tape;
        }

        public static async Task<AllPartitionsRecorder> GetAllEvents(this IStore store)
        {
            var tape = new AllPartitionsRecorder();
            await store.ReadAllAsync(0, tape).ConfigureAwait(false);
            return tape;
        }

        public static async Task<Recorder> GetAllChunksForAPartition(this IStore store, String partitionId)
        {
            var tape = new Recorder();
            await store.ReadForwardAsync(partitionId, tape).ConfigureAwait(false);
            return tape;
        }
    }
}
