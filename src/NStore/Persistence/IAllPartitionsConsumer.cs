using System.Threading.Tasks;

namespace NStore.Persistence
{
    public interface IAllPartitionsConsumer
    {
        Task<ScanAction> Consume(
            long position,
            string partitionId,
            long index,
            object payload
        );
    }
}