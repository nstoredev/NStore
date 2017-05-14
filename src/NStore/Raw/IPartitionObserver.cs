namespace NStore.Raw
{
    public interface IPartitionObserver
    {
        ScanCallbackResult Observe(
            long partitionIndex, 
            object payload
        );
    }

	public interface IStoreObserver
	{
		ScanCallbackResult Observe(
			long storeIndex,
			string streamId,
			long partitionIndex,
			object payload
		);
	}
}