using System;
using System.Collections.Generic;

namespace NStore.Raw
{
    public class StoreRecorder : IStoreConsumer
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

        public ScanAction Consume(long storeIndex, string partitionId, long idx, object payload)
        {
            _data.Add(new Element(storeIndex, partitionId, idx, payload));
            _map[idx] = payload;
            return ScanAction.Continue;
        }

        public void Replay(Action<long, string, long, object> action, int startAt = 0)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                var d = _data[i];
                action(d.StoreIndex, d.PartitionId, d.Index, d.Payload);
            }
        }

        public object this[int position] => _data[position].Payload;
    }
}