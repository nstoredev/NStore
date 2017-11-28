using Xunit;

namespace NStore.Domain.Tests.PocoAggregateTests
{
    public class PocoAggregateTests : AbstractPocoAggregateTest<Mage>
    {
        private readonly Attack _attack = new Attack("target_1", Attack.AttackLevel.Hit);

        [Fact]
        public void changeset_should_be_empty()
        {
            Assert.Empty(Events);
        }

        [Fact]
        public void changeset_should_have_two_spells()
        {
            Aggregate.Do(_attack);

            Assert.Equal(0.1, State.ExperienceLevel);

            Assert.Collection(Events,
                evt =>
                {
                    Assert.IsType<SpellCast>(evt);
                    Assert.Equal("Shield", ((SpellCast) evt).Name);
                },
                evt =>
                {
                    Assert.IsType<SpellCast>(evt);
                    Assert.Equal("Fireball", ((SpellCast) evt).Name);
                }
            );
        }

        [Fact]
        public void should_handle_single_return_value()
        {
            Aggregate.Do(new Hide()); 
            Assert.False(State.IsVisible);
        }
    }
}