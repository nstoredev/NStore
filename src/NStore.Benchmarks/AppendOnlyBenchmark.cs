using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.InMemory;
using NStore.Raw;

namespace NStore.Benchmarks
{
    [CoreJob]
    public class AppendOnlyBenchmark
    {
        [Benchmark]
        public void load_memory()
        {
            var store = new InMemoryRawStore();
            Worker(store, 4, 3000);
        }

        private void Worker(IRawStore store, int dop, int number)
        {
            int max = number;
            var range = Enumerable.Range(0, max);

            range.ForEachAsync(dop, i =>
                store.PersistAsync("Stream_1", i, new {data = "this is a test"})
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}