using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using NStore.Core.Persistence;
using NStore.Persistence.MsSql;
using NStore.Persistence.MsSql.Tests;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private MsSqlPersistence _sqlPersistence;
        private MsSqlPersistenceOptions _options;
        private string _connectionString;
        private const string TestSuitePrefix = "Mssql";

        protected void Connect()
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

        protected internal IPersistence Create(bool dropOnInit)
        {
            Connect();

            _logger.LogInformation("Starting test #{number}", _testRunId);

            _options = CreateOptions();

            _sqlPersistence = new MsSqlPersistence(_options);
            if (dropOnInit)
            {
                _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            }
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
