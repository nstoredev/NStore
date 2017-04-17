using System.Collections.Generic;

namespace NStore.Tests
{
	class Accumulator
	{
		private IList<object> _data = new List<object>();
		public IEnumerable<object> Data => _data;
		public int Length => _data.Count;

		public ScanCallbackResult Consume(long idx, object payload)
		{
			_data.Add(payload);
			return ScanCallbackResult.Continue;
		}

		public object this[int index]
		{
			get
			{
				return _data[index];
			}
		}
	}
}
