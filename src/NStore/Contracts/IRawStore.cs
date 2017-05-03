using System;
using System.Threading.Tasks;

namespace NStore.Contracts
{
	public interface IRawStore
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

        /// <summary>
        /// Delete a stream by id
        /// </summary>
        /// <param name="streamId">Stream id</param>
        /// <returns>Task</returns>
        /// @@TODO delete invalid stream should throw or not?
	    Task DeleteAsync(string streamId);
	}
}