﻿using Newtonsoft.Json;
using NStore.Core.Persistence;
using NStore.Persistence.LiteDB;

// [assembly: CollectionBehavior(DisableTestParallelization = true)]

// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BaseStoreTest
    {
        private const string TestSuitePrefix = "LiteDB";

        protected IStore Create(bool dropOnInit)
        {
            var pathToFile = $"{_testRunId}.litedb";

            _logger.LogInformation("Starting test #{number}", _testRunId);
            var serializer = new LiteDBSerializer();
            var options = new LiteDBStoreOptions(serializer, LoggerFactory)
            {
                ConnectionString = pathToFile,
                StreamsCollectionName = "streams"
            };

            var lite = new LiteDbStore(options);

            if (dropOnInit)
            {
                lite.DeleteDataFiles();
            }

            lite.Init();

            return lite;
        }

        private static void Clear(IStore store, bool drop)
        {
            var lite = (LiteDbStore)store;
            if (drop)
            {
                lite.DeleteDataFiles();
            }

            lite.Dispose();
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