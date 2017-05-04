using System;
using System.Collections.Generic;
using NStore.Raw.Contracts;

namespace NStore.Tests
{
    internal class Accumulator
    {
        private readonly IList<object> _data = new List<object>();
        private readonly IDictionary<long, object> _map = new Dictionary<long, object>();
        public IEnumerable<object> Data => _data;
        public int Length => _data.Count;

        public ScanCallbackResult Consume(long idx, object payload)
        {
            _data.Add(payload);
            _map[idx] = payload;
            return ScanCallbackResult.Continue;
        }

        public object this[int index] => _data[index];
        public object ByIndex(int index) => _map[index];
        public bool IsEmpty => _data.Count == 0;

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