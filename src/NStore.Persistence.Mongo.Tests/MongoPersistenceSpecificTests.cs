using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
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
            using var cts = new CancellationTokenSource(2000);
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
                    clusterConfigurator.Subscribe<CommandSucceededEvent>(_ => callCount++);
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

    public abstract class insert_id_already_existing_base : BasePersistenceTest
    {
        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            var options = base.GetMongoPersistenceOptions();
            options.UseLocalSequence = GetUseLocalSequence();
            options.SequenceCollectionName = "sequence_test";
            return options;
        }

        protected abstract bool GetUseLocalSequence();

        [Fact]
        public async Task resilient_to_multiple_persistence_write_concurrently()
        {
            IPersistence store2 = Create(false);
            string partition1 = Guid.NewGuid().ToString();
            string partition2 = Guid.NewGuid().ToString();

            await Store.AppendAsync(partition1, 1, new { data = "first attempt" }).ConfigureAwait(false);
            //now store2 inserts a chunk with another id 
            await store2.AppendAsync(partition2, 1, new { data = "first attempt" }).ConfigureAwait(false);

            var chunk = await Store.AppendAsync(partition1, 2, new { data = "second data" }).ConfigureAwait(false);
            Assert.Equal(3, chunk.Position);
        }
    }

    public class insert_id_already_existing_base_local_sequence : insert_id_already_existing_base
    {
        protected override bool GetUseLocalSequence()
        {
            return true;
        }
    }

    public class insert_id_already_existing_base_db_sequence : insert_id_already_existing_base
    {
        protected override bool GetUseLocalSequence()
        {
            return false;
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

    public abstract class batch_append_with_position_conflict_base : BasePersistenceTest
    {
        protected internal override MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            var options = base.GetMongoPersistenceOptions();
            options.UseLocalSequence = GetUseLocalSequence();
            options.SequenceCollectionName = "sequence_batch_test";
            return options;
        }

        protected abstract bool GetUseLocalSequence();

        [Fact]
        public async Task should_retry_only_failed_chunks_on_position_conflict()
        {
            if (Batcher == null)
                return;

            // Create a second store to simulate concurrent writes
            var store2 = Create(false);

            var partition1 = Guid.NewGuid().ToString();
            var partition2 = Guid.NewGuid().ToString();

            // First write to establish position
            await Store.AppendAsync(partition1, 1, new { data = "first" }).ConfigureAwait(false);

            // Prepare batch jobs
            var jobs = new[]
            {
                new WriteJob(partition1, 2, new { data = "job1" }, null),
                new WriteJob(partition2, 1, new { data = "job2" }, null),
                new WriteJob(partition1, 3, new { data = "job3" }, null),
                new WriteJob(partition2, 2, new { data = "job4" }, null),
            };

            // Simulate position conflict by writing with store2 during the batch operation
            // This will cause some positions to be already taken
            var writeTask = Task.Run(async () =>
            {
                await Task.Delay(50).ConfigureAwait(false);
                await store2.AppendAsync(partition2, 3, new { data = "concurrent" }).ConfigureAwait(false);
            });

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None).ConfigureAwait(false);
            await writeTask.ConfigureAwait(false);

            // All jobs should eventually succeed
            Assert.All(jobs, job => Assert.Equal(WriteJob.WriteResult.Committed, job.Result));
            Assert.All(jobs, job => Assert.True(job.Position > 0));

            // Verify all chunks are in the database
            var chunk1 = await Store.ReadSingleBackwardAsync(partition1, 2, CancellationToken.None);
            var chunk2 = await Store.ReadSingleBackwardAsync(partition2, 1, CancellationToken.None);
            var chunk3 = await Store.ReadSingleBackwardAsync(partition1, 3, CancellationToken.None);
            var chunk4 = await Store.ReadSingleBackwardAsync(partition2, 2, CancellationToken.None);

            Assert.NotNull(chunk1);
            Assert.NotNull(chunk2);
            Assert.NotNull(chunk3);
            Assert.NotNull(chunk4);
        }

        [Fact]
        public async Task should_handle_mixed_failures_correctly()
        {
            if (Batcher == null)
                return;

            var partition = Guid.NewGuid().ToString();

            // First write to establish baseline
            await Store.AppendAsync(partition, 1, new { data = "first" }).ConfigureAwait(false);

            var jobs = new[]
            {
                new WriteJob(partition, 2, new { data = "job1" }, null),
                new WriteJob(partition, 2, new { data = "duplicate_index" }, null), // Will fail with DuplicatedIndex
                new WriteJob(partition, 3, new { data = "job3" }, "op1"),
                new WriteJob(partition, 4, new { data = "job4" }, "op1"), // Will fail with DuplicatedOperation
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None).ConfigureAwait(false);

            // Check results
            Assert.True(jobs[0].Position > 0);
            Assert.Equal(WriteJob.WriteResult.Committed, jobs[0].Result);

            Assert.Equal(0, jobs[1].Position);
            Assert.Equal(WriteJob.WriteResult.DuplicatedIndex, jobs[1].Result);

            Assert.True(jobs[2].Position > 0);
            Assert.Equal(WriteJob.WriteResult.Committed, jobs[2].Result);

            Assert.Equal(0, jobs[3].Position);
            Assert.Equal(WriteJob.WriteResult.DuplicatedOperation, jobs[3].Result);

            // Verify correct data in database
            var chunk2 = await Store.ReadSingleBackwardAsync(partition, 2, CancellationToken.None);
            var chunk3 = await Store.ReadSingleBackwardAsync(partition, 3, CancellationToken.None);

            Assert.NotNull(chunk2);
            Assert.NotNull(chunk3);
            Assert.Equal("job1", ((dynamic)chunk2.Payload).data);
            Assert.Equal("job3", ((dynamic)chunk3.Payload).data);
        }
    }

    public class batch_append_with_position_conflict_local_sequence : batch_append_with_position_conflict_base
    {
        protected override bool GetUseLocalSequence()
        {
            return true;
        }
    }

    public class batch_append_with_position_conflict_db_sequence : batch_append_with_position_conflict_base
    {
        protected override bool GetUseLocalSequence()
        {
            return false;
        }
    }
}