using NStore.Core.Persistence;
using System;
using System.Threading.Tasks;

namespace NStore.Persistence.Tests
{
    public static class PersistenceDebugExtensions
    {
        public static async Task<AllPartitionsRecorder> GetAllEventForAPartition(this IPersistence persistence, String partitionId)
        {
            var tape = new AllPartitionsRecorder();
            await persistence.ReadForwardAsync(partitionId, tape).ConfigureAwait(false);
            return tape;
        }

        public static async Task<AllPartitionsRecorder> GetAllEvents(this IPersistence persistence)
        {
            var tape = new AllPartitionsRecorder();
            await persistence.ReadAllAsync(0, tape).ConfigureAwait(false);
            return tape;
        }

        public static async Task<Recorder> GetAllChunksForAPartition(this IPersistence persistence, String partitionId)
        {
            var tape = new Recorder();
            await persistence.ReadForwardAsync(partitionId, tape).ConfigureAwait(false);
            return tape;
        }
    }
}
