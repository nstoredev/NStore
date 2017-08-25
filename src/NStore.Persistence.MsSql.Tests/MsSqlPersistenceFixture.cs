using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using NStore.Persistence.MsSql;
using NStore.Persistence.MsSql.Tests;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private MsSqlPersistence _sqlPersistence;
        private MsSqlPersistenceOptions _options;
        private string ConnectionString;
        private const string TestSuitePrefix = "Mssql";

        protected void Connect()
        {
            ConnectionString = Environment.GetEnvironmentVariable("NSTORE_MSSQL");
            if (String.IsNullOrWhiteSpace(ConnectionString))
                throw new TestMisconfiguredException("Please set connection string as NSTORE_MSSQL environment variable");

            if (ConnectionString.StartsWith("\""))
                ConnectionString = ConnectionString.Substring(1);

            if (ConnectionString.EndsWith("\""))
                ConnectionString = ConnectionString.Substring(0, ConnectionString.Length - 1);
        }

        private IPersistence Create()
        {
            Connect();

            _logger.LogInformation("Starting test #{number}", _testRunId);

            _options = new MsSqlPersistenceOptions(LoggerFactory)
            {
                ConnectionString = ConnectionString,
                StreamsTableName = "streams_" + _testRunId + "_" + GetType().Name,
                Serializer = new JsonMsSqlSerializer()
            };

            _sqlPersistence = new MsSqlPersistence(_options);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _sqlPersistence.InitAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Test #{number} started", _testRunId);
            return _sqlPersistence;
        }

        private void Clear()
        {
            _logger.LogInformation("Cleaning up test #{number}", _testRunId);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Cleanup test #{number} done", _testRunId);
        }
    }
}
