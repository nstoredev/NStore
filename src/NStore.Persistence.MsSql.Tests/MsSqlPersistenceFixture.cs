using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Newtonsoft.Json;
using NStore.Persistence.MsSql;


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public class JsonMsSqlSerializer : IMsSqlPayloadSearializer
    {
        JsonSerializerSettings Settings { get; set; }

        public JsonMsSqlSerializer()
        {
            this.Settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            };
        }

        public object Deserialize(string serialized)
        {
            return JsonConvert.DeserializeObject(serialized, Settings);
        }

        public string Serialize(object payload)
        {
            return JsonConvert.SerializeObject(payload, Settings);
        }
    }

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
                throw new Exception("Please set connection string as NSTORE_MSSQL environment variable");
            //                "Server=(local)\\SqlExpress;Database=NStore;Trusted_Connection=True;MultipleActiveResultSets=true";
            // docker
            //            ConnectionString = "Server=localhost,1433;Database=NStore;User Id=sa;Password=NStoreD0ck3r;MultipleActiveResultSets=true";
            Console.WriteLine($"Connected to {ConnectionString}");
        }

        private IPersistence Create()
        {
            _id = Interlocked.Increment(ref _staticId);

            _options = new MsSqlPersistenceOptions(LoggerFactory)
            {
                ConnectionString = ConnectionString,
                StreamsTableName = "streams_" + _id + "_" + GetType().Name,
                Serializer = new JsonMsSqlSerializer()
            };

            _sqlPersistence = new MsSqlPersistence(_options);
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            _sqlPersistence.InitAsync(CancellationToken.None).Wait();
            Console.WriteLine($"Connected and ready {_options.StreamsTableName}");
            return _sqlPersistence;
        }

        private void Clear()
        {
            _sqlPersistence.DestroyAllAsync(CancellationToken.None).Wait();
            Console.WriteLine($"Cleared {_options.StreamsTableName}");
        }
    }
}
