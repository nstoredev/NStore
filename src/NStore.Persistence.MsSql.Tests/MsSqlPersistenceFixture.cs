using System;
using System.Threading;
using NStore.Core.Persistence;
using NStore.Persistence.MsSql;
using NStore.Persistence.MsSql.Tests;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private string _connectionString;
        private const string TestSuitePrefix = "Mssql";

        private void Connect()
        {
            _connectionString = Environment.GetEnvironmentVariable("NSTORE_MSSQL");
            if (String.IsNullOrWhiteSpace(_connectionString))
            {
                throw new TestMisconfiguredException("Please set connection string as NSTORE_MSSQL environment variable");
            }

            if (_connectionString.StartsWith("\""))
            {
                _connectionString = _connectionString.Substring(1);
            }

            if (_connectionString.EndsWith("\""))
            {
                _connectionString = _connectionString.Substring(0, _connectionString.Length - 1);
            }
        }

        protected virtual MsSqlPersistenceOptions CreateOptions()
        {
            return new MsSqlPersistenceOptions(LoggerFactory)
            {
                ConnectionString = _connectionString,
                StreamsTableName = "streams_" + _testRunId + "_" + GetType().Name,
                Serializer = new JsonMsSqlSerializer()
            };
        }

        protected IPersistence Create(bool dropOnInit)
        {
            Connect();

            _logger.LogInformation("Starting test #{number}", _testRunId);

            var options = CreateOptions();

            var sqlPersistence = new MsSqlPersistence(options);
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
            var sql = (MsSqlPersistence) persistence;
            if (drop)
            {
                _logger.LogInformation("Cleaning up test #{number}", _testRunId);
                sql.DestroyAllAsync(CancellationToken.None).Wait();
                _logger.LogInformation("Cleanup test #{number} done", _testRunId);
            }
        }
    }
}
