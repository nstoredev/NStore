using NStore.Core.Persistence;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Persistence.Tests
{
    public abstract partial class BaseConcurrencyTests : BaseStoreTest
    {
        private readonly IStore _store2;
        protected IStore Store2 { get; }

        protected BaseConcurrencyTests() : base()
        {
            _store2 = Create(false);
            Store2 = new LogDecorator(_store2, LoggerFactory);
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

    public class When_Write_To_Same_Stream_From_Multiple_Repositories_Read_Backward : BaseConcurrencyTests
    {
        [Fact(Skip = "probably not useful by refactor. Automatic index proposition was moved to Stream")]
        public async Task Verify_that_index_is_always_equal_to_id_when_Append_chunk_without_explicit_index()
        {
            // Repo1 writes to a stream (no index specified)
            await Store.AppendAsync("test1", -1, "CHUNK1").ConfigureAwait(false);

            // Repo2 writes to another stream.
            await Store2.AppendAsync("test2", -1, "Stuff not interesting").ConfigureAwait(false);
            await Store2.AppendAsync("test1", -1, "CHUNK2").ConfigureAwait(false);

            // Repo1 write again on Test1, but in memory index is wrong. WE expect index to be ok
            await Store.AppendAsync("test1", -1, "CHUNK3").ConfigureAwait(false);

            Recorder rec = new Recorder();
            await Store.ReadBackwardAsync("test1", long.MaxValue, rec).ConfigureAwait(false);

            Assert.Equal("CHUNK3", rec.Data.ElementAt(0));
            Assert.Equal("CHUNK2", rec.Data.ElementAt(1));
            Assert.Equal("CHUNK1", rec.Data.ElementAt(2));
        }
    }
}
