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
            var mapper = new BsonMapper();
            mapper.RegisterType<SnapshotInfo>
            (
                serialize: (snapshot) => JsonConvert.SerializeObject(snapshot),
                deserialize: DeserializeSnapshot
            );

            var pathToFile = $"{_testRunId}.litedb";

            if (dropOnInit)
            {
                if (File.Exists(pathToFile))
                    File.Delete(pathToFile);
            }

            _logger.LogInformation("Starting test #{number}", _testRunId);
            var serializer = new LiteDBSerializer();
            _options = new LiteDBPersistenceOptions(serializer, LoggerFactory, mapper)
            {
                ConnectionString = pathToFile,
                StreamsCollectionName = "streams"
            };

            _liteDbPersistence = new LiteDBPersistence(_options);
            _liteDbPersistence.InitAsync(CancellationToken.None).Wait();

            return _liteDbPersistence;
        }

        private SnapshotInfo DeserializeSnapshot(BsonValue bson)
        {
            return JsonConvert.DeserializeObject<SnapshotInfo>(bson.AsString);
        }

        private void Clear()
        {
            _liteDbPersistence.DestroyAllAsync(CancellationToken.None).Wait();
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
            var wrapped = new Wrapper
            {
                Value = payload,
                Type = payload?.GetType().FullName ?? "null"
            };
            return JsonConvert.SerializeObject(wrapped, Settings);
        }

        public object Deserialize(string payload)
        {
            var wrapped = JsonConvert.DeserializeObject<Wrapper>(payload, Settings);
            return wrapped.Value;
        }
    }
}