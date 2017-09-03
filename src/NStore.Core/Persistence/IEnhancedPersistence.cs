using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    public sealed class WriteJob : IChunk
    {
        // in
        public string PartitionId { get; private set; }
        public long Index { get; private set; }
        public object Payload { get; private set; }
        public string OperationId { get; private set; }

        // out
        public long Position { get; private set; }
        public Exception Exception  { get; private set; }
        
        public WriteJob(string partitionId, long index, object payload, string operationId)
        {
            PartitionId = partitionId;
            Index = index;
            Payload = payload;
            OperationId = operationId;
        }

        public void Succeded(long position)
        {
            this.Position = position;
        }

        public void Failed(Exception ex)
        {
            this.Exception = ex;
        }
    }

    public interface IEnhancedPersistence
    {
        Task AppendBatchAsync(
            WriteJob[] queue,
            CancellationToken cancellationToken    
        );
    }
}