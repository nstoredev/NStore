using System.IO;
using System.Threading;
using NStore.Core.Persistence;
using NStore.Persistence.Sqlite;
using NStore.Persistence.Sqlite.Tests;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private string ConnectionString;
        private const string TestSuitePrefix = "Sqlite";

        private void Connect()
        {
            var pathToFile = $"{_testRunId}.db";
            ConnectionString = $"Data Source={pathToFile}";

            if (File.Exists(pathToFile))
            {
                File.Delete(pathToFile);
            }
        }

        protected IPersistence Create(bool dropOnInit)
        {
            Connect();

            _logger.LogInformation("Starting test #{number}", _testRunId);

            var options = new SqlitePersistenceOptions(LoggerFactory)
            {
                ConnectionString = ConnectionString,
                StreamsTableName = "streams_" + _testRunId + "_" + GetType().Name,
                Serializer = new JsonSqliteSerializer()
            };

            var sqlPersistence = new SqlitePersistence(options);
            if (dropOnInit)
            {
                sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            }

            sqlPersistence.InitAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Test #{number} started", _testRunId);
            return sqlPersistence;
        }

        protected void Clear(IPersistence persistence, bool drop)
        {
            if (!drop)
            {
                return;
            }

            var sql = (SqlitePersistence) persistence;
            _logger.LogInformation("Cleaning up test #{number}", _testRunId);
            sql.DestroyAllAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Cleanup test #{number} done", _testRunId);
        }
    }
}