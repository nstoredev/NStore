using NStore.InMemory;
using NStore.Raw;

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        public IRawStore Create()
        {
            var store = new InMemoryRawStore();
            return store;
        }

        public void Clear()
        {

        }
    }
}