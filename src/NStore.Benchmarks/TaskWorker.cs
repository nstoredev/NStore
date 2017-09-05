using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Benchmarks
{
    public static class TaskWorker
    {
        public static Task Run(IPersistence store, IEnumerable<int> iterations)
        {
            var list = new List<Task>();

            foreach (var iteration in iterations)
            {
                list.Add(store.AppendAsync("Stream_1", iteration, new { data = "this is a test" }));
            }

            return Task.WhenAll(list.ToArray());
        }
    }
}