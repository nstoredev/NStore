using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Tpl
{
    public class AsyncWriteJob : WriteJob
    {
        private readonly TaskCompletionSource<IChunk> _completionSource = 
            new TaskCompletionSource<IChunk>();

        public Task<IChunk> Task => _completionSource.Task;

        public AsyncWriteJob(string partitionId, long index, object payload, string operationId) : 
            base(partitionId, index, payload, operationId)
        {
        }

        public override void Succeeded(IChunk chunk)
        {
            base.Succeeded(chunk);
            this._completionSource.SetResult(chunk);
        }

        public override void Failed(WriteResult result)
        {
            base.Failed(result);
            this._completionSource.SetResult(null);
            //            _completionSource.SetException(new InvalidStreamOperationException());
        }
    }
}