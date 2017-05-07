using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
	public interface IRawStoreLifecycle
	{
		Task InitAsync();
		Task DestroyStoreAsync();
	}

	public interface IRawStore
	{
		/// <summary>
		/// Scan partition
		/// </summary>
		/// <param name="partitionId"></param>
		/// <param name="indexStart"></param>
		/// <param name="direction"></param>
		/// <param name="consume"></param>
		/// <param name="limit"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		Task ScanAsync(
			string partitionId,
			long indexStart, 
			ScanDirection direction, 
			Func<long, object, ScanCallbackResult> consume,
			int limit = int.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
		);

		/// <summary>
		/// Scan full store
		/// </summary>
		/// <param name="sequenceStart">starting id (included) </param>
		/// <param name="direction">Scan direction</param>
		/// <param name="consume">Consumer </param>
		/// <param name="limit">Max items</param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		Task ScanStoreAsync(
			long sequenceStart,
			ScanDirection direction,
			Func<long, object, ScanCallbackResult> consume,
			int limit = int.MaxValue,
			CancellationToken cancellationToken = default(CancellationToken)
		);

		/// <summary>
		/// Persist a chunk in partition
		/// </summary>
		/// <param name="partitionId"></param>
		/// <param name="index"></param>
		/// <param name="payload"></param>
		/// <param name="operationId"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		Task PersistAsync(
			string partitionId,
			long index, 
			object payload, 
			string operationId = null,
			CancellationToken cancellationToken = default(CancellationToken)
		);

		/// <summary>
		/// Delete a partition by id
		/// </summary>
		/// <param name="partitionId">Stream id</param>
		/// <param name="fromIndex">From index</param>
		/// <param name="toIndex">to Index</param>
		/// <param name="cancellationToken"></param>
		/// <returns>Task</returns>
		/// @@TODO delete invalid stream should throw or not?
		Task DeleteAsync(
			string partitionId,
			long fromIndex = 0,
			long toIndex = long.MaxValue,
			CancellationToken cancellationToken = default(CancellationToken)
		);
	}
}