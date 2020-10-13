using Fasterflect;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Linq;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using System;
using System.Collections.Generic;
using System.Linq;
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

    public class TestMongoPayloadSerializer : IMongoPayloadSerializer
    {
        public List<Object> SerializedPayloads { get; set; } = new List<object>();

        public List<Object> DeserializedPayloads { get; set; } = new List<object>();

        public object Deserialize(object payload)
        {
            DeserializedPayloads.Add(payload);
            return payload;
        }

        public object Serialize(object payload)
        {
            SerializedPayloads.Add(payload);
            return payload;
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

    public class When_using_custom_payload_serializer : BasePersistenceTest
    {
        private TestMongoPayloadSerializer _testMongoPayloadSerializer;

        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            //Add your custom payload serializer.
            var options = base.GetMongoPersistenceOptions();
            if (_testMongoPayloadSerializer == null)
            {
                _testMongoPayloadSerializer = new TestMongoPayloadSerializer();
            }
            options.MongoPayloadSerializer = _testMongoPayloadSerializer;
            return options;
        }

        [Fact()]
        public async Task Verify_payload_serializer_is_called_for_basic_append_and_read()
        {
            // Write to a simple stream and verify that indeed the wrapping is happening.
            await Store.AppendAsync("test1", 1, "CHUNK1").ConfigureAwait(false);
            Assert.Equal("CHUNK1", _testMongoPayloadSerializer.SerializedPayloads.Single());

            //read from the stream
            await Store.GetAllChunksForAPartition("test1").ConfigureAwait(false);
            Assert.Equal("CHUNK1", _testMongoPayloadSerializer.DeserializedPayloads.Single());
        }

        [Fact()]
        public async Task Verify_payload_serializer_is_called_for_backward_read()
        {
            await Store.AppendAsync("test1", 1, "CHUNK1").ConfigureAwait(false);

            await Store.ReadSingleBackwardAsync("test1").ConfigureAwait(false);
            Assert.Equal("CHUNK1", _testMongoPayloadSerializer.SerializedPayloads.Single());

            //verify that reading a non existent we do not throw exception and no serializer is called
            var result = await Store.ReadSingleBackwardAsync("non-existent").ConfigureAwait(false);
            Assert.Equal("CHUNK1", _testMongoPayloadSerializer.DeserializedPayloads.Single());

            //this is useful to verify that null can be returned without throwing any null exception
            Assert.Null(result);

            //now use another api, for backward reading.
            await Store.ReadBackwardAsync("test1", long.MaxValue, EmptySubscription).ConfigureAwait(false);
            Assert.Equal(new List<object>() { "CHUNK1" , "CHUNK1" },  _testMongoPayloadSerializer.DeserializedPayloads);

            await Store.ReadBackwardAsync("non-existent", long.MaxValue, EmptySubscription).ConfigureAwait(false);
            Assert.Equal(new List<object>() { "CHUNK1", "CHUNK1" }, _testMongoPayloadSerializer.DeserializedPayloads);
        }

        [Fact()]
        public async Task Verify_payload_serializer_is_called_for_read_by_operation_async()
        {
            var operationId = Guid.NewGuid().ToString();
            await Store.AppendAsync("test1", 1, "CHUNK1", operationId).ConfigureAwait(false);

            await Store.ReadByOperationIdAsync("test1", operationId, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal("CHUNK1", _testMongoPayloadSerializer.SerializedPayloads.Single());
        }

        [Fact()]
        public async Task Verify_payload_serializer_is_called_for_read_all_by_operation_async()
        {
            var operationId = Guid.NewGuid().ToString();
            await Store.AppendAsync("test1", 1, "CHUNK1", operationId).ConfigureAwait(false);
            //remember to use different partition id or the second operation will be idempotent
            await Store.AppendAsync("test3", 1, "CHUNK2", operationId).ConfigureAwait(false);

            //now read all by operation id, we should have two deserialization
            await Store.ReadAllByOperationIdAsync(operationId, EmptySubscription);
            Assert.Equal(new List<object>() { "CHUNK1", "CHUNK2" }, _testMongoPayloadSerializer.DeserializedPayloads);
        }
    }

    public class Can_intercept_mongo_query_with_options : BasePersistenceTest
    {
        private Int32 callCount;

        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            var options = base.GetMongoPersistenceOptions();
            options.CustomizePartitionClientSettings = mongoClientSettings =>
                mongoClientSettings.ClusterConfigurator = clusterConfigurator =>
                {
                    clusterConfigurator.Subscribe<CommandSucceededEvent>(e =>
                    {
                        callCount++;
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

    public class Verify_race_condition_that_caused_memory_leak : BasePersistenceTest
    {
        private MongoPersistenceOptions _persistenceOptions;

        protected override IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            _persistenceOptions = options;
            return new MongoPersistence<CustomChunk>(options);
        }

        private class TestMongoPersistence : MongoPersistence<CustomChunk>
        {
            private Int32 _loop_blocker = 0;

            public TestMongoPersistence(MongoPersistenceOptions options) : base(options)
            {
            }

            protected override string GetExceptionMessage(MongoWriteException ex, CustomChunk chunk)
            {
                var collection = (IMongoCollection<CustomChunk>)this.GetFieldValue("_chunks");
                if (_loop_blocker++ > 10)
                {
                    throw new Exception("Loop in inserting stuff into database");
                }

                if (ex.Message.Contains("_id_")) 
                {
                    //We need to simulate the fact that the id for partition is primary 
                    var partitionIndexConflict = collection
                        .AsQueryable()
                        .Where(c => c.PartitionId == chunk.PartitionId && c.Index == chunk.Index)
                        .Any();
                    if (partitionIndexConflict)
                    {
                        return "Error in partition_index";
                    }
                }
                return base.GetExceptionMessage(ex, chunk);
            }
        }

        [Fact]
        public async Task Race_condition_fill_empty()
        {
            var persistence1 = new MongoPersistence<CustomChunk>(_persistenceOptions);
            await persistence1.InitAsync(CancellationToken.None).ConfigureAwait(false);

            var partitionId = Guid.NewGuid().ToString();
            //Create a stream
            await persistence1.AppendAsync(partitionId, "Evento1").ConfigureAwait(false);

            //Call internal append to simulate race condition
            var lastPosition = await persistence1.ReadLastPositionAsync().ConfigureAwait(false);

            var chunk1 = new CustomChunk();
            chunk1.Init(
              lastPosition + 1,
              partitionId,
              2,
              "Evento conflittante 1",
              Guid.NewGuid().ToString()
            );

            var chunk2 = new CustomChunk();
            chunk2.Init(
              lastPosition + 1, //Conflicting
              partitionId,
              2, //Conflicting
              "Evento conflittante 2",
              Guid.NewGuid().ToString()
            );

            //This insert the first chunk
            var task = (Task<IChunk>)persistence1.CallMethod("InternalPersistAsync", new object[] { chunk1, CancellationToken.None });
            task.Wait();

            //this should not generate loop.
            task = (Task<IChunk>)persistence1.CallMethod("InternalPersistAsync", new object[] { chunk2, CancellationToken.None });
            try
            {
                task.Wait();
                throw new Exception("We are expecting an exception");
            }
            catch (AggregateException aex)
            {
                Assert.IsType<DuplicateStreamIndexException>(aex.InnerException);
            }
        }

        [Fact]
        public async Task Race_condition_with_empty()
        {
            var persistence1 = new TestMongoPersistence(_persistenceOptions);
            await persistence1.InitAsync(CancellationToken.None).ConfigureAwait(false);

            var _chunks = (IMongoCollection<CustomChunk>)persistence1.GetFieldValue("_chunks");

            _chunks.Database.DropCollection(_chunks.CollectionNamespace.CollectionName);

            //Call internal append to simulate race condition
            var lastPosition = await persistence1.ReadLastPositionAsync().ConfigureAwait(false);

            var chunk1 = new CustomChunk();
            chunk1.Init(
                lastPosition + 1,
                "::empty",
                lastPosition + 1,
                null,
                Guid.NewGuid().ToString()
            );

            var chunk2 = new CustomChunk();
            chunk2.Init(
                lastPosition + 1,
                "::empty",
                lastPosition + 1,
                null,
                Guid.NewGuid().ToString()
            );

            //This insert the first chunk
            var task = (Task<IChunk>)persistence1.CallMethod("InternalPersistAsync", new object[] { chunk1, CancellationToken.None });
            task.Wait();

            task = (Task<IChunk>)persistence1.CallMethod("InternalPersistAsync", new object[] { chunk2, CancellationToken.None });
            try
            {
                task.Wait();
                throw new Exception("We are expecting an exception");
            }
            catch (AggregateException aex)
            {
                Assert.IsType<DuplicateStreamIndexException>(aex.InnerException);
            }
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