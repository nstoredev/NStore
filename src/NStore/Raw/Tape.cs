using System;
using System.Collections.Generic;

namespace NStore.Raw
{
    public class Tape : IPartitionObserver
    {
        private sealed class Element
        {
            public Element(long index, object payload)
            {
                Index = index;
                Payload = payload;
            }

            public long Index { get; }
            public object Payload { get; }
        }

        private readonly IList<Element> _data = new List<Element>();
        private readonly IDictionary<long, object> _map = new Dictionary<long, object>();
        public IEnumerable<object> Data => _data;
        public int Length => _data.Count;

        public ScanCallbackResult Observe(long idx, object payload)
        {
            _data.Add(new Element(idx, payload));
            _map[idx] = payload;
            return ScanCallbackResult.Continue;
        }

        public void Replay(Action<object> action, int startAt = 0)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                action(_data[i].Payload);
            }
        }

        public bool IsEmpty => _data.Count == 0;
        public object this[int position] => _data[position].Payload;

        public long GetIndex(int position) => _data[position].Index;
        public object ByIndex(int index) => _map[index];

        //@@TODO refactor with dumper on Replay
        public void Dump()
        {
            int counter = 0;
            Console.WriteLine("Dumping accumulator");
            foreach (Element d in _data)
            {
                Console.WriteLine($"    {counter++:0000} => {d}");
            }
            Console.WriteLine("done.");
        }
    }


    public class SuperTape : IStoreObserver
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
		public IEnumerable<object> Data => _data;
		public int Length => _data.Count;

		public ScanCallbackResult Observe(long storeIndex, string partitionId, long idx, object payload)
		{
            _data.Add(new Element(storeIndex, partitionId, idx, payload));
			_map[idx] = payload;
			return ScanCallbackResult.Continue;
		}

		public void Replay(Action<object> action, int startAt = 0)
		{
			for (var i = startAt; i < _data.Count; i++)
			{
				action(_data[i].Payload);
			}
		}

		public bool IsEmpty => _data.Count == 0;
		public object this[int position] => _data[position].Payload;

		public long GetIndex(int position) => _data[position].Index;
		public object ByIndex(int index) => _map[index];
	}
}