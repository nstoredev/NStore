using System.Threading.Tasks;

namespace NStore.Streams
{
    public interface IStream
    {
        Task Append(string payload, string operationId = null);
    }
}