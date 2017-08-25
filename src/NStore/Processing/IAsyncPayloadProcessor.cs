using System.Threading.Tasks;

namespace NStore.Processing
{
    public interface IAsyncPayloadProcessor
    {
        Task ProcessAsync(object payload);
    }
}