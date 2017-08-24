using System.Threading.Tasks;

namespace NStore.Processing
{
    public interface IPayloadProcessor
    {
        void Process(object payload);
    }

    public interface IAsyncPayloadProcessor
    {
        Task ProcessAsync(object payload);
    }
}