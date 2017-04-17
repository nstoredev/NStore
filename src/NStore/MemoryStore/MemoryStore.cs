using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace NStore
{
	public class MemoryStream
	{
	}

	public class MemoryStore : IStore
	{
		private ConcurrentDictionary<string,MemoryStream> _data = new ConcurrentDictionary<string, MemoryStream>();

		public MemoryStore()
		{
		}

		public Task GetAsync(string streamId, long indexStart, Action<long, object> callback)
		{
			if(_data.TryGetValue(streamId, out MemoryStream ms))
			{


			}

			return Task.FromResult(0);
		}

		public Task InitAsync()
		{
			throw new NotImplementedException();
		}

		public Task PersistAsync(string streamId, long index, object payload, string operationId)
		{
			throw new NotImplementedException();
		}

	}
}
