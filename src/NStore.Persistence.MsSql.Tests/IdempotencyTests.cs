using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using Xunit;

namespace NStore.Persistence.MsSql.Tests
{
    public class IdempotencyTests : BaseStoreTest
    {
        protected override MsSqlStoreOptions CreateOptions()
        {
            var opts = base.CreateOptions();
            opts.StreamIdempotencyEnabled = false;
            return opts;
        }

        [Fact]
        public async Task should_persist_with_null_operationId()
        {
            var chunk1 = await Store.AppendAsync
            (
                "partition", 
                1, 
                "payload 1", 
                null, 
                CancellationToken.None
            );
            
            var reader = new Recorder();
            await Store.ReadAllAsync(0, reader, 100, CancellationToken.None);

            Assert.Collection(reader.Chunks, chunk =>
            {
                Assert.Null(chunk.OperationId);
            });
            
        }

        [Fact]
        public async Task should_append_chunk_with_duplicated_operation_id()
        {
            var chunk1 = await Store.AppendAsync("partition", 1, "payload 1", "same_op", CancellationToken.None);
            var chunk2 = await Store.AppendAsync("partition", 2, "payload 2", "same_op", CancellationToken.None);
            
            Assert.NotNull(chunk1);
            Assert.NotNull(chunk2);
            
            Assert.Equal(1, chunk1.Position);
            Assert.Equal(2, chunk2.Position);
        }

        [Fact]
        public async Task reading_stream_by_operation_id_should_throw()
        {
            var ex = await Assert.ThrowsAsync<MsSqlStoreException>(async () =>
            {
                await Store.ReadByOperationIdAsync("partition", "anyop", CancellationToken.None);
            });
            
            Assert.Equal("Stream idempotency is disabled. Cannot search by OperationId", ex.Message);
        }
        
        [Fact]
        public async Task reading_store_by_operation_id_should_throw()
        {
            var ex = await Assert.ThrowsAsync<MsSqlStoreException>(async () =>
            {
                await Store.ReadAllByOperationIdAsync("anyop", NullSubscription.Instance, CancellationToken.None);
            });
            
            Assert.Equal("Stream idempotency is disabled. Cannot search by OperationId", ex.Message);
        }
    }
}