using System;
using System.Threading.Tasks;

namespace NStore.Contracts
{
	public interface IStore
	{
		//@@TODO move outside
		Task InitAsync();
		//@@TODO move outside
		Task DestroyStoreAsync();

		Task ScanAsync(
			string streamId, 
			long indexStart, 
			ScanDirection direction, 
			Func<long, object, ScanCallbackResult> callback,
			int limit = int.MaxValue
		);

		Task PersistAsync(
			string streamId, 
			long index, 
			object payload, 
			string operationId = null
		);
	}
}