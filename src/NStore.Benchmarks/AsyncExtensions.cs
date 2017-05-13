using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NStore.Benchmarks
{
    internal static class AsyncExtensions
    {
        public static Task ForEachAsync<T>(
            this IEnumerable<T> source, int dop, Func<T, Task> body)
        {
            return Task.WhenAll(
                from partition in Partitioner.Create(source).GetPartitions(dop)
                select Task.Run((Func<Task>) async delegate
                {
                    using (partition)
                        while (partition.MoveNext())
                            await body(partition.Current)
                                .ContinueWith(t =>
                                {
                                    //observe exceptions
                                });
                }));
        }
    }
}