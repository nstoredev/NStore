using NStore.Core.Persistence;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Persistence.Tests
{
    public abstract partial class BaseConcurrencyTests : BasePersistenceTest
    {
        protected readonly IPersistence _persistence2;
        protected IPersistence Store2 { get; }

        protected BaseConcurrencyTests() : base()
        {
            _persistence2 = Create(false);
            Store2 = new LogDecorator(_persistence2, LoggerFactory);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // nothing to do
            }
            base.Dispose(disposing);
        }
    }

    public class When_Write_To_Same_Stream_From_Multiple_Repositories : BaseConcurrencyTests
    {
        [Fact(Skip="Evaluate if this test is ok")]
        public async Task Without_specifying_the_Index_DuplicateStreamIndexException_Should_Occur()
        {
            // Repo1 writes to the stream (no index specified)
            await Store.AppendAsync("test", 1).ConfigureAwait(false);

            // Repo2 writes to the same stream (no index specified)
            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store2.AppendAsync("test", 2)
            ).ConfigureAwait(false);

            Assert.Equal("Duplicated index 1 on stream test", ex.Message);
            Assert.Equal("test", ex.StreamId);
            Assert.Equal(1, ex.StreamIndex);
        }

        [Fact(Skip = "Evaluate if this test is ok")]
        public async Task Retrying_Write_After_a_DuplicateStreamIndexException_Should_Work()
        {
            // Repo1 writes to the stream (no index specified)
            await Store.AppendAsync("test", 1).ConfigureAwait(false);

            // Repo2 writes to the same stream (no index specified)
            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store2.AppendAsync("test", 2)
            ).ConfigureAwait(false);

            Assert.Equal("Duplicated index 1 on stream test", ex.Message);
            Assert.Equal("test", ex.StreamId);
            Assert.Equal(1, ex.StreamIndex);

            // Repo2 retries to write the same chunk again
            await Store2.AppendAsync("test", 2).ConfigureAwait(false);
        }

        [Fact(Skip = "Evaluate if this test is ok")]
        public async Task Multiple_Writes_On_Same_Stream_From_Multiple_Repositories()
        {
            // Repo1 writes to the stream (no index specified)
            await Store.AppendAsync("test", 1).ConfigureAwait(false);

            // Repo2 writes to the same stream (no index specified)
            var ex = await Assert.ThrowsAnyAsync<DuplicateStreamIndexException>(() =>
                Store2.AppendAsync("test", 2)
            ).ConfigureAwait(false);

            Assert.Equal("Duplicated index 1 on stream test", ex.Message);
            Assert.Equal("test", ex.StreamId);
            Assert.Equal(1, ex.StreamIndex);

            // Repo2 retries to write the same chunk again
            await Store2.AppendAsync("test", 2).ConfigureAwait(false);

            // Repo1 tries to wrote something again
            await Store.AppendAsync("test", 3).ConfigureAwait(false);

            // Read the data back
            // I need an inspector to check the streams
            var inspector = Create(false);
            var tape = await inspector.GetAllEventForAPartition("test").ConfigureAwait(false);
            Assert.Equal(3, tape.Length);
            Assert.Equal(1, tape[0]);
            Assert.Equal(2, tape[1]);
            Assert.Equal(3, tape[2]);
        }
    }
}
