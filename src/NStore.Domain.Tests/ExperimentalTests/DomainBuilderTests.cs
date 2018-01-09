using System.Linq;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Domain.Experimental;
using NStore.Domain.Tests.PocoAggregateTests;
using Xunit;

// ReSharper disable ClassNeverInstantiated.Global
namespace NStore.Domain.Tests.ExperimentalTests
{
    public class UserState
    {
        public bool IsValidUser { get; private set; }

        private void On(UserRegistered e)
        {
            IsValidUser = true;
        }
    }

    public class UserRegistered
    {
        public string Name { get; private set; }

        public UserRegistered(string name)
        {
            Name = name;
        }
    }

    public class User : Aggregate<UserState>
    {
        public void Register(string name)
        {
            Emit(new UserRegistered(name));
        }
    }

    public class DomainBuilderTests
    {
        private readonly IPersistence _memory = new InMemoryPersistence(new InMemoryPersistenceOptions());

        [Fact]
        public async Task syntactic_sugar_comes_at_rescue()
        {
            var ecommerce = new DomainBuilder()
                .PersistOn(() => _memory)
                .CreateAggregatesWith(() => new DefaultAggregateFactory())
                .Build();

            await ecommerce.MutateAsync<User>("user_123", user =>
            {
                user.Register("Super Mario");
            });

            var o = await _memory.ReadSingleBackwardAsync("user_123");
            Assert.IsType<Changeset>(o.Payload);
            Assert.IsType<UserRegistered>(((Changeset) o.Payload).Events.First());
        }

        [Fact]
        public async Task more_syntactic_sugar_comes_at_rescue()
        {
            var strangerStreams = new DomainBuilder()
                .PersistOn(() => _memory)
                .WithSnapshotsOn(() => new DefaultSnapshotStore(new InMemoryPersistence(new InMemoryPersistenceOptions())))
                .CreateAggregatesWith(() => new DefaultAggregateFactory())
                .Build();

            var scene = strangerStreams.Record("scene120");
            var el = await scene.GetAsync<Mage>("11");
            el.Do(new Attack("demogorgon", Attack.AttackLevel.Kill));
            await scene.StreamAsync();

            var changes = await strangerStreams.OpenForRead("11").RecordAsync();
            Assert.Equal(1, changes.Length);
        }

        [Fact]
        public async Task should_broadcast_mutations_to_processor()
        {
            long viewCounter = 0;

            var strangerStreams = new DomainBuilder()
                .PersistOn(() => _memory)
                .WithSnapshotsOn(() => new DefaultSnapshotStore(new InMemoryPersistence(new InMemoryPersistenceOptions())))
                .CreateAggregatesWith(() => new DefaultAggregateFactory())
                .BroadcastTo(Watcher)
                .Build();
            
            Task<bool> Watcher(IChunk c)
            {
                if (c.PartitionId == "views")
                {
                    viewCounter++;
                }
                return Task.FromResult(true);
            }

            await strangerStreams.PushAsync("views", 1).ConfigureAwait(false);
            
            await Task.Delay(500);
            await strangerStreams.ShutdownAsync().ConfigureAwait(false);
            
            Assert.Equal(1, viewCounter);
        }
    }
}