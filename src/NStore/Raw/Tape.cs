using System;
using System.Collections.Generic;
using NStore.Raw.Contracts;

namespace NStore.Raw
{
    public class Tape
    {
        private readonly IList<object> _data = new List<object>();
        private readonly IDictionary<long, object> _map = new Dictionary<long, object>();
        public IEnumerable<object> Data => _data;
        public int Length => _data.Count;

        public ScanCallbackResult Record(long idx, object payload)
        {
            _data.Add(payload);
            _map[idx] = payload;
            return ScanCallbackResult.Continue;
        }

        public void Replay(Action<object> action, int startAt = 0)
        {
            for (var i = startAt; i < _data.Count; i++)
            {
                action(_data[i]);
            }
        }

        public object this[int index] => _data[index];
        public object ByIndex(int index) => _map[index];
        public bool IsEmpty => _data.Count == 0;

        //@@TODO refactor with dumper on Replay
        public void Dump()
        {
            int counter = 0;
            Console.WriteLine("Dumping accumulator");
            foreach (object d in _data)
            {
                Console.WriteLine($"    {counter++:0000} => {d}");
            }
            Console.WriteLine("done.");
        }
    }
}