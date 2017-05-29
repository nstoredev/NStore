using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;
using NStore.InMemory;
using NStore.Persistence;

namespace NStore.Benchmarks
{
    [SimpleJob(launchCount: 1, warmupCount: 3, targetCount: 2, invocationCount: 5, id: "append_job")]
    public class ParallelWritesBenchmark
    {
        private readonly IEnumerable<int> _iterations = Enumerable.Range(1, 1000);

        public int Workers { get; set; } = 8;

        [Benchmark]
        public void for_each_async()
        {
            var store = (IPersistence) new InMemoryPersistence(new ReliableNetworkSimulator(1, 1));
            _iterations.ForEachAsync(Workers, i =>
                store.PersistAsync("Stream_1", i, new {data = "this is a test"})
            ).ConfigureAwait(false).GetAwaiter().GetResult();
        }


        [Benchmark]
        public void task_when_all()
        {
            var store = (IPersistence) new InMemoryPersistence(new ReliableNetworkSimulator(1, 1));
            var all = _iterations.Select(i =>
                store.PersistAsync("Stream_1", i, new {data = "this is a test"}));
            Task.WhenAll(all).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void task_wait_all()
        {
            var store = (IPersistence) new InMemoryPersistence(new ReliableNetworkSimulator(1, 1));
            var all = _iterations.Select(i =>
                store.PersistAsync("Stream_1", i, new {data = "this is a test"}));
            Task.WaitAll(all.ToArray());
        }
    }
}