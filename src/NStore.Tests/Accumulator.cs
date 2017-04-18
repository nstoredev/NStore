using System.Collections.Generic;

namespace NStore.Tests
{
	internal class Accumulator
	{
		private readonly IList<object> _data = new List<object>();
		public IEnumerable<object> Data => _data;
		public int Length => _data.Count;

		public ScanCallbackResult Consume(long idx, object payload)
		{
			_data.Add(payload);
			return ScanCallbackResult.Continue;
		}

		public object this[int index] => _data[index];
	}
}
