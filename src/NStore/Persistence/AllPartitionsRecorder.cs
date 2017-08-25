using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class AllPartitionsRecorder : ISubscription
    {
        private readonly IList<IChunk> _data = new List<IChunk>();
        public int Length => _data.Count;

        public void Replay(Action<long, string, long, object> action)
        {
            Replay(action, 0);
        }

        public Task OnStartAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public void Replay(Action<long, string, long, object> action, int startAt)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                var d = _data[i];
                action(d.Position, d.PartitionId, d.Index, d.Payload);
            }
        }
        
        public void Replay(Action<IChunk> action)
        {
            Replay(action, 0);
        }

        public void Replay(Action<IChunk> action, int startAt)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                action(_data[i]);
            }
        }

        public object this[int position] => _data[position].Payload;

        public Task<bool> OnNextAsync(IChunk chunk)
        {
            _data.Add(chunk);
            return Task.FromResult(true);
        }

        public Task CompletedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task StoppedAsync(long indexOrPosition)
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(long indexOrPosition, Exception ex)
        {
            throw ex;
        }
    }
}