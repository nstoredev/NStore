using NStore.Core.Persistence;
using System;
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
        [Fact]
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
    }
}
