using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Persistence.Mongo.Tests
{
    public class CustomChunk : MongoChunk
    {
        public DateTime CreateAt { get; private set; }

        [BsonDictionaryOptions(DictionaryRepresentation.ArrayOfArrays)]
        public IDictionary<string, string> CustomHeaders { get; set; } =
            new Dictionary<string, string>();

        public CustomChunk()
        {
            this.CreateAt = new DateTime(2017, 1, 1, 10, 12, 13).ToUniversalTime();
            this.CustomHeaders["test.1"] = "a";
        }
    }

    public class mongo_persistence_with_custom_chunk_type : BasePersistenceTest
    {
        protected override IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            return new MongoPersistence<CustomChunk>(options);
        }

        [Fact]
        public async Task can_write_custom_data()
        {
            var persisted = (CustomChunk)await Store.AppendAsync("a", 1, "data");

            var collection = GetCollection<CustomChunk>();
            var read_from_collection = await (await collection.FindAsync(FilterDefinition<CustomChunk>.Empty)).FirstAsync();

            Assert.Equal("a", read_from_collection.CustomHeaders["test.1"]);
            Assert.Equal(persisted.CreateAt, read_from_collection.CreateAt);
        }

        [Fact]
        public async Task can_read_custom_data()
        {
            var persisted = (CustomChunk)await Store.AppendAsync("a", 1, "data");
            var read = (CustomChunk)await Store.ReadSingleBackwardAsync("a");

            Assert.Equal("a", read.CustomHeaders["test.1"]);
            Assert.Equal(persisted.CreateAt, read.CreateAt);
        }
    }

    public class empty_payload_serialization : BasePersistenceTest
    {
        public class SerializerSpy : IMongoPayloadSerializer
        {
            public int SerializeCount { get; private set; }
            public int DeserializeCount { get; private set; }

            public object Serialize(object payload)
            {
                SerializeCount++;
                return payload;
            }

            public object Deserialize(object payload)
            {
                DeserializeCount++;
                return payload;
            }
        }

        private SerializerSpy _serializer;

        protected override IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            _serializer = new SerializerSpy();
            options.MongoPayloadSerializer = _serializer;
            return new MongoPersistence(options);
        }

        [Fact]
        public async Task empty_payload_should_be_serialized()
        {
            await Store.AppendAsync("a", 1, "payload").ConfigureAwait(false);
            await Assert.ThrowsAsync<DuplicateStreamIndexException>(() =>
                Store.AppendAsync("a", 1, "payload")
            ).ConfigureAwait(false);

            // Counter progression
            // 1 first ok
            // 2 second ko
            // 3 empty 
            Assert.Equal(3, _serializer.SerializeCount);
        }
    }

    public class filler_tests : BasePersistenceTest
    {
        [Fact]
        public async Task filler_should_regenerate_operation_id()
        {
            await Store.AppendAsync("::empty", 1, "payload", "op1").ConfigureAwait(false);
            var cts = new CancellationTokenSource(2000);
            var result = await Store.AppendAsync("::empty", 2, "payload", "op1", cts.Token).ConfigureAwait(false);
            Assert.Null(result);

            var recorder = new Recorder();
            await Store.ReadAllAsync(0, recorder, 100).ConfigureAwait(false);

            Assert.Collection(recorder.Chunks,
                c => Assert.Equal("op1", c.OperationId),
                c => Assert.Equal("_2", c.OperationId)
            );
        }
    }

    public class Can_intercept_mongo_query_with_options : BasePersistenceTest
    {
        private Int32 callCount;
        private Int32 failedCallCount;
        private Int32 startCallCount;

        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            var options = base.GetMongoPersistenceOptions();
            options.CustomizePartitionClientSettings = mongoClientSettings =>
                mongoClientSettings.ClusterConfigurator = clusterConfigurator =>
                {
                    clusterConfigurator.Subscribe<CommandStartedEvent>(e =>
                    {
                        startCallCount++;
                    });
                    clusterConfigurator.Subscribe<CommandSucceededEvent>(e =>
                    {
                        callCount++;
                    });
                    clusterConfigurator.Subscribe<CommandFailedEvent>(e =>
                    {
                        failedCallCount++;
                    });
                };
            return options;
        }

        [Fact()]
        public async Task Verify_that_after_append_async_we_have_intercepted_the_call()
        {
            callCount = 0;

            // Repo1 writes to a stream
            await Store.AppendAsync("test1", 1, "CHUNK1").ConfigureAwait(false);

            Assert.Equal(1, callCount);
        }
    }

    /// <summary>
    /// Correctly initialize the seed when you want to use the sequence generated it
    /// </summary>
    public class Sequence_generator_id_is_initialized_correctly : BasePersistenceTest
    {
        private MongoPersistenceOptions _options;

        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            _options = base.GetMongoPersistenceOptions();
            _options.UseLocalSequence = false;
            _options.SequenceCollectionName = "sequence_test";
            return _options;
        }

        [Fact()]
        public void Verify_that_after_persistence_initialization_sequence_collection_is_populated()
        {
            // We need to be sure that the record was correctly created
            var url = new MongoUrl(_options.PartitionsConnectionString);
            var client = new MongoClient(url);
            var db = client.GetDatabase(url.DatabaseName);
            var coll = db.GetCollection<BsonDocument>(_options.SequenceCollectionName);

            var sequenceDocument = coll.AsQueryable().SingleOrDefault();
            Assert.NotNull(sequenceDocument);
            Assert.Equal("streams", sequenceDocument["_id"].AsString);
            Assert.Equal(0L, sequenceDocument["LastValue"].AsInt64);
        }
    }
}