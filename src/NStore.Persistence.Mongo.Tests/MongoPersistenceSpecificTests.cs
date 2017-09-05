using System;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using NStore.Tpl;
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
            await Store.AppendAsync("a", 1, "payload");
            await Assert.ThrowsAsync<DuplicateStreamIndexException>(() =>
                Store.AppendAsync("a", 1, "payload")
            );

            // Counter progression
            // 1 first ok
            // 2 second ko
            // 3 empty 
            Assert.Equal(3, _serializer.SerializeCount);
        }
    }


    public class batch_writes_test : BasePersistenceTest
    {
        private IEnhancedPersistence Batcher => (IEnhancedPersistence)_mongoPersistence;

        [Fact]
        public async Task should_add_many()
        {
            var jobs = new[]
            {
                new WriteJob("a", 1, "first", null),
                new WriteJob("a", 2, "second", null),
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            Assert.InRange(jobs[0].Position, 1, 2);
            Assert.InRange(jobs[1].Position, 1, 2);
        }

        [Fact]
        public async Task should_add_many_in_random_order()
        {
            var jobs = new[]
            {
                new WriteJob("a", -1, "first", null),
                new WriteJob("a", -1, "second", null),
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            Assert.InRange(jobs[0].Position, 1, 2);
            Assert.InRange(jobs[1].Position, 1, 2);
        }

        [Fact]
        public async Task should_fail_on_adding_many()
        {
            var jobs = new[]
            {
                new WriteJob("a", 1, "call me maybe", null),
                new WriteJob("a", 1, "call me maybe", null),
                new WriteJob("a", 2, "me too", "fail"),
                new WriteJob("a", 3, "me too", "fail"),
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            Assert.Equal(1, jobs[0].Position);
            Assert.Equal(2, jobs[1].Position);
            Assert.Equal(3, jobs[2].Position);
            Assert.Equal(4, jobs[3].Position);

            var firstIndexResults = new[] { jobs[0].Result, jobs[1].Result };

            Assert.Contains(firstIndexResults, result => result == WriteJob.WriteResult.Committed);
            Assert.Contains(firstIndexResults, result => result == WriteJob.WriteResult.DuplicatedIndex);

            var secondIndexResults = new[] { jobs[2].Result, jobs[3].Result };
            Assert.Contains(secondIndexResults, result => result == WriteJob.WriteResult.Committed);
            Assert.Contains(secondIndexResults, result => result == WriteJob.WriteResult.DuplicatedOperation);

            var a1 = await Store.ReadSingleBackwardAsync("a", 1, CancellationToken.None);
            var a2 = await Store.ReadSingleBackwardAsync("a", 2, CancellationToken.None);

            Assert.NotNull(a1);
            Assert.NotNull(a2);

            Assert.Equal("call me maybe", a1.Payload);
            Assert.Equal("me too", a2.Payload);
        }

        [Fact]
        public async Task async_write_jobs()
        {
            // note: insert order is not guaranteed, failures can append odd rows
            var jobs = new[]
            {
                new AsyncWriteJob("a", 1, "first", null),
                new AsyncWriteJob("a", 1, "fail here", null),
                new AsyncWriteJob("a", 2, "second", "fail"),
                new AsyncWriteJob("a", 3, "fail here too", "fail"),
            };

            Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            var allTasks = jobs.Select(x => x.Task).ToArray();
            var written = await Task.WhenAll(allTasks);

            Assert.True(4 == written.Length);
            Assert.NotNull(written[0]);
            Assert.Null(written[1]);
            Assert.NotNull(written[2]);
            Assert.Null(written[3]);
        }

        [Fact]
        public async Task write_with_batcher()
        {
            var cts = new CancellationTokenSource(10_000);
            var batcher = new PersistenceBatcher(_mongoPersistence);
            batcher.Cancel(10_000);
            
            await batcher.AppendAsync("a", 1, "first",null, cts.Token);
//            await Assert.ThrowsAsync<DuplicateStreamIndexException>(() => batcher.AppendAsync("a", 1, "fail here"));

            var lastPos = await Store.ReadLastPositionAsync();
            
            Assert.Equal(1, lastPos);
        }
    }
}