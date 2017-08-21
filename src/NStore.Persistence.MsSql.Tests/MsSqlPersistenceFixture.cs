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
        private static readonly string ConnectionString;
        private static int _staticId = 0;
        private int _id;
        private const string TestSuitePrefix = "Mssql";

        static BasePersistenceTest()
        {
            ConnectionString = Environment.GetEnvironmentVariable("NSTORE_MSSQL");
            if (String.IsNullOrWhiteSpace(ConnectionString))
                throw new TestMisconfiguredException("Please set connection string as NSTORE_MSSQL environment variable");

            if (ConnectionString.StartsWith("\""))
                ConnectionString = ConnectionString.Substring(1);

            if (ConnectionString.EndsWith("\""))
                ConnectionString = ConnectionString.Substring(0, ConnectionString.Length - 1);

            Console.WriteLine($"Connected to {ConnectionString}");
        }

        private IPersistence Create()
        {
            _id = Interlocked.Increment(ref _staticId);
            _logger.LogInformation("Starting test #{number}", _id);

            _options = new MsSqlPersistenceOptions(LoggerFactory)
            {
                ConnectionString = ConnectionString,
                StreamsTableName = "streams_" + _id + "_" + GetType().Name,
                Serializer = new JsonMsSqlSerializer()
            };

            _sqlPersistence = new MsSqlPersistence(_options);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _sqlPersistence.InitAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Test #{number} started", _id);
            return _sqlPersistence;
        }

        private void Clear()
        {
            _logger.LogInformation("Cleaning up test #{number}", _id);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _logger.LogInformation("Cleanup test #{number} done", _id);
        }
    }
}
