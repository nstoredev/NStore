using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using NStore.Core.Logging;
using NStore.Persistence.Tests;
using Xunit;
using Xunit.Abstractions;

namespace NStore.Persistence.LiteDB.Tests
{
    public class PersistenceOptimizationsTests
    {
        private readonly ITestOutputHelper _output;

        public PersistenceOptimizationsTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact(Skip = "file size optimization tests, on demand")]
        public async Task fill_size()
        {
            var mapper = new BsonMapper();

            mapper.Entity<LiteDBChunk>()
                .Field(x => x.Index, "i")
                .Field(x => x.OperationId, "o")
                .Field(x => x.PartitionId, "s")
                .Field(x => x.Position, "po")
                .Field(x => x.StreamOperation, "so")
                .Field(x => x.StreamSequence, "se")
                .Field(x => x.Payload, "pa");

            var options = new LiteDBPersistenceOptions
            (
                new LiteDBSerializer(),
                NStoreNullLoggerFactory.Instance,
                mapper
            )
            {
                ConnectionString = "test_for_size.litedb"
            };

            if (File.Exists(options.ConnectionString))
            {
                File.Delete(options.ConnectionString);
            }

            var store = new LiteDbStore(options);
            store.DeleteDataFiles();

            store.Init();

            var tasks = Enumerable.Range(1, 10_000).
                Select(x=> store.AppendAsync(
                    $"{x:D10}",
                    x, 
                    null, 
                    null,
                    CancellationToken.None
                )
            );

            await Task.WhenAll(tasks);

            store.Dispose();

            var fi = new FileInfo(options.ConnectionString);

            _output.WriteLine($"File size is {fi.Length} bytes");
        }
    }
}
