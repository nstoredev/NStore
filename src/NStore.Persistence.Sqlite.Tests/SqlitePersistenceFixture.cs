using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NStore.Core.Persistence;
using NStore.Persistence.Sqlite;
using NStore.Persistence.Sqlite.Tests;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private SqlitePersistence _sqlPersistence;
        private SqlitePersistenceOptions _options;
        private string ConnectionString;
        private const string TestSuitePrefix = "Sqlite";

        protected void Connect()
        {
            //            ConnectionString = "Data Source= :memory: ; Cache = shared";

            var pathToFile = $"{_testRunId}.db";
            ConnectionString = $"Data Source={pathToFile}";

            if (File.Exists(pathToFile))
                File.Delete(pathToFile);

            //            ConnectionString = Environment.GetEnvironmentVariable("NSTORE_SQLITE");
            //            if (String.IsNullOrWhiteSpace(ConnectionString))
            //                throw new TestMisconfiguredException("Please set connection string as NSTORE_SQLITE environment variable");
            //
            //            if (ConnectionString.StartsWith("\""))
            //                ConnectionString = ConnectionString.Substring(1);
            //
            //            if (ConnectionString.EndsWith("\""))
            //                ConnectionString = ConnectionString.Substring(0, ConnectionString.Length - 1);
        }

        protected internal IPersistence Create(bool dropOnInit)
        {
            Connect();

            _logger.LogInformation("Starting test #{number}", _testRunId);

            _options = new SqlitePersistenceOptions(LoggerFactory)
            {
                ConnectionString = ConnectionString,
                StreamsTableName = "streams_" + _testRunId + "_" + GetType().Name,
                Serializer = new JsonSqliteSerializer()
            };

            _sqlPersistence = new SqlitePersistence(_options);
            if (dropOnInit)
            {
                _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            }
            _sqlPersistence.InitAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Test #{number} started", _testRunId);
            return _sqlPersistence;
        }

        protected internal void Clear()
        {
            _logger.LogInformation("Cleaning up test #{number}", _testRunId);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Cleanup test #{number} done", _testRunId);
        }
    }
}
