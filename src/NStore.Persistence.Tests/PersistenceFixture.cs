using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NStore.Raw;
using Xunit;
// ReSharper disable InconsistentNaming

namespace NStore.Persistence.Tests
{
    public abstract partial class BasePersistenceTest : IDisposable
    {
        public IRawStore Store { get; }

        protected BasePersistenceTest()
        {
            Store = Create();
        }

        public void Dispose()
        {
            Clear();
        }
    }

    public class WriteTests : BasePersistenceTest
    {
        [Fact]
        public async Task can_insert_at_first_index()
        {
            await Store.PersistAsync("Stream_1", 1, new {data = "this is a test"});
        }
    }

    public class negative_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_persist_with_chunk_id()
        {
            await Store.PersistAsync("Stream_Neg", -1, "payload");

            var acc = new Tape();
            await Store.ScanAsync("Stream_Neg", 0, ScanDirection.Forward, acc.Record);
            Assert.Equal("payload", acc.ByIndex(1));
        }

    }

    public class insert_at_last_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_work()
        {
            await Store.PersistAsync("Stream_1", long.MaxValue, new {data = "this is a test"});
        }

    }

    public class insert_duplicate_chunk_index : BasePersistenceTest
    {
        [Fact]
        public async Task should_throw()
        {
            await Store.PersistAsync("dup", 1, new {data = "first attempt"});
            await Store.PersistAsync("dup", 2, new {data = "should not work"});

            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store.PersistAsync("dup", 1, new {data = "this is a test"})
            );

            Assert.Equal("Duplicated index 1 on stream dup", ex.Message);
            Assert.Equal("dup", ex.StreamId);
            Assert.Equal(1, ex.Index);
        }

    }

    public class long_running_test : BasePersistenceTest
    {
        [Fact(Skip = "long running")]
        //[Fact]
        public async Task InsertMany()
        {
            await Worker(1, 10000);
            await Worker(2, 10000);
            await Worker(3, 10000);
            await Worker(4, 10000);
            await Worker(5, 10000);
            await Worker(6, 10000);
            await Worker(7, 10000);
            await Worker(8, 10000);
            await Worker(9, 10000);
            await Worker(10, 10000);
            await Worker(20, 10000);
        }

        private async Task Worker(int dop, int number)
        {
            int max = number;
            var range = Enumerable.Range(0, max);
            var sw = new Stopwatch();

            sw.Start();

            await range.ForEachAsync(dop, i =>
                Store.PersistAsync("Stream_2", i, new {data = "this is a test"})
            );

            sw.Stop();

            Console.WriteLine($"Written {max} chunks in {sw.ElapsedMilliseconds}ms using {dop} workers");
        }
    }

    public static class AsyncExtensions
    {
        public static Task ForEachAsync<T>(
            this IEnumerable<T> source, int dop, Func<T, Task> body)
        {
            return Task.WhenAll(
                from partition in Partitioner.Create(source).GetPartitions(dop)
                select Task.Run(async delegate
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