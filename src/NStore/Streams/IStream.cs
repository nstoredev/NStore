using System;
using System.Threading.Tasks;
using NStore.Raw;

namespace NStore.Streams
{
    public interface IStream
    {
        Task Append(string payload, string operationId = null);
        Task Read(int index, Func<long, object, ScanCallbackResult> consumer);
        Task Delete();
    }
}