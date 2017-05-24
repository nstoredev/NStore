using System.Threading.Tasks;

namespace NStore.Raw
{
    public interface IStoreConsumer
    {
        Task<ScanAction> Consume(
            long storeIndex,
            string streamId,
            long partitionIndex,
            object payload
        );
    }
}