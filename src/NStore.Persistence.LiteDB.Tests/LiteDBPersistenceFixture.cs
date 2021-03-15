using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using LiteDB;
using Newtonsoft.Json;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Persistence;
using NStore.Persistence.LiteDB;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]


// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private LiteDBPersistenceOptions _options;
        private LiteDBPersistence _liteDbPersistence;
        private const string TestSuitePrefix = "LiteDB";

        protected IPersistence Create(bool dropOnInit)
        {
            var pathToFile = $"{_testRunId}.litedb";


            _logger.LogInformation("Starting test #{number}", _testRunId);
            var serializer = new LiteDBSerializer();
            _options = new LiteDBPersistenceOptions(serializer, LoggerFactory)
            {
                ConnectionString = pathToFile,
                StreamsCollectionName = "streams"
            };

            _liteDbPersistence = new LiteDBPersistence(_options);

            if (dropOnInit)
            {
                _liteDbPersistence.DeleteDataFiles();
            }

            _liteDbPersistence.Init();

            return _liteDbPersistence;
        }

        private SnapshotInfo DeserializeSnapshot(BsonValue bson)
        {
            return JsonConvert.DeserializeObject<SnapshotInfo>(bson.AsString);
        }

        private void Clear()
        {
            _liteDbPersistence.DeleteDataFiles();
            _liteDbPersistence.Dispose();
        }
    }

    public class LiteDBSerializer : ILiteDBPayloadSerializer
    {
        private JsonSerializerSettings Settings { get; set; }

        private class Wrapper
        {
            public string Type { get; set; }
            public object Value { get; set; }
        }

        public LiteDBSerializer()
        {
            this.Settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            };
        }

        public string Serialize(object payload)
        {
            if (payload == null)
            {
                return null;
            }

            var wrapped = new Wrapper
            {
                Value = payload,
                Type = payload?.GetType().FullName ?? "null"
            };
            return JsonConvert.SerializeObject(wrapped, Settings);
        }

        public object Deserialize(string payload)
        {
            if (payload == null)
            {
                return null;
            }

            var wrapped = JsonConvert.DeserializeObject<Wrapper>(payload, Settings);
            return wrapped.Value;
        }
    }
}