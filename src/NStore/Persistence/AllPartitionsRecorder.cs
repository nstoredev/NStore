using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class AllPartitionsRecorder : ISubscription
    {
        private sealed class Element
        {
            public Element(long storeIndex, string partitionId, long index, object payload)
            {
                StoreIndex = storeIndex;
                PartitionId = partitionId;
                Index = index;
                Payload = payload;
            }

            public long StoreIndex { get; }
            public string PartitionId { get; }
            public long Index { get; }
            public object Payload { get; }
        }

        private readonly IList<Element> _data = new List<Element>();
        private readonly IDictionary<long, object> _map = new Dictionary<long, object>();
        public int Length => _data.Count;

        public void Replay(Action<long, string, long, object> action)
        {
            Replay(action, 0);
        }

        public void Replay(Action<long, string, long, object> action, int startAt)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                var d = _data[i];
                action(d.StoreIndex, d.PartitionId, d.Index, d.Payload);
            }
        }

        public object this[int position] => _data[position].Payload;

        public Task<bool> OnNext(IChunk data)
        {
            _data.Add(new Element(data.Position , data.PartitionId, data.Index, data.Payload));
            _map[data.Index] = data.Payload;
            return Task.FromResult(true);
        }

        public Task Completed()
        {
            return Task.CompletedTask;
        }

        public Task OnError(Exception ex)
        {
            throw ex;
        }
    }
}