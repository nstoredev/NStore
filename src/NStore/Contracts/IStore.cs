using System;
using System.Threading.Tasks;

namespace NStore
{

	public enum ScanDirection
	{
		Forward,
		Backward
	}

	public enum ScanCallbackResult
	{
		Stop,
		Continue
	}

	public class DuplicateStreamIndexException : Exception
	{
		public long Index { get; }
		public string StreamId { get; }

		public DuplicateStreamIndexException(string streamId, long index) : 
			base($"Duplicated index {index} on stream {streamId}")
		{
			this.Index = index;
			this.StreamId = streamId;
		}
	}

	public interface IStore
	{
		Task InitAsync();
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