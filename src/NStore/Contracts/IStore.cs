using System;
using System.Threading.Tasks;

namespace NStore
{
	public interface IStore
	{
		Task InitAsync();
		Task GetAsync(string streamId, long indexStart, Action<long,object> callback);
		Task PersistAsync(string streamId, long index, object payload);
	}
}