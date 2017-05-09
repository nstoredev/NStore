using System.Threading.Tasks;

namespace NStore.Raw
{
    public interface IRawStoreLifecycle
    {
        Task InitAsync();
        Task DestroyStoreAsync();
    }
}