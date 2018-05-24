using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
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
            var persisted = (CustomChunk)await Store.AppendAsync("a", "data");

            var collection = GetCollection<CustomChunk>();
            var read_from_collection = await (await collection.FindAsync(FilterDefinition<CustomChunk>.Empty)).FirstAsync();

            Assert.Equal("a", read_from_collection.CustomHeaders["test.1"]);
            Assert.Equal(persisted.CreateAt, read_from_collection.CreateAt);
        }

        [Fact]
        public async Task can_read_custom_data()
        {
            var persisted = (CustomChunk)await Store.AppendAsync("a", "data");
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
            var result = await Store.AppendAsync("::empty", 2, "payload", "op1",cts.Token).ConfigureAwait(false);
            Assert.Null(result);

            var recorder = new Recorder();
            await Store.ReadAllAsync(0, recorder, 100).ConfigureAwait(false);

            Assert.Collection(recorder.Chunks, 
                c=> Assert.Equal("op1", c.OperationId),
                c=> Assert.Equal("_2", c.OperationId)
            );
        }
    }

    public class When_Write_To_Same_Stream_From_Multiple_Repositories : BaseConcurrencyTests
    {
        [Fact()]
        public async Task Verify_that_index_is_always_equal_to_id_when_Append_chunk_without_explicit_index()
        {
            // Repo1 writes to a stream (no index specified)
            await Store.AppendAsync("test1", -1, "CHUNK1").ConfigureAwait(false);

            // Repo2 writes to another stream.
            await Store2.AppendAsync("test2", -1, "Stuff not interesting").ConfigureAwait(false);

            // Repo1 write again on Test1, but in memory index is wrong. WE expect index to be ok
            await Store.AppendAsync("test1", -1, "CHUNK2").ConfigureAwait(false);

            Recorder rec = new Recorder();
            await Store.ReadForwardAsync("test1", rec).ConfigureAwait(false);

            foreach (var chunk in rec.Chunks)
            {
                Assert.Equal(chunk.Position, chunk.Index);
            }
        }
    }
}