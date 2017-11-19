using System.Threading.Tasks;

namespace NStore.LoadTests
{
    public interface IConsumer<in T>
    {
        Task<bool> ReceiveAsync(T msg);
    }
}