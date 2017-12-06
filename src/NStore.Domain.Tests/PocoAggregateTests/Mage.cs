// ReSharper disable ClassNeverInstantiated.Global
using Events = System.Collections.Generic.IEnumerable<object>;

namespace NStore.Domain.Tests.PocoAggregateTests
{
    public class Mage
    {
        private int SpellCount { get; set; }
        public bool IsVisible { get; private set; } = true;
        public double ExperienceLevel => SpellCount / 20.0;

        private void On(SpellCast e)
        {
            this.SpellCount++;
        }

        private void On(Hidden e)
        {
            this.IsVisible = false;
        }

        public object Do(Hide command)
        {
            return new Hidden();
        }

        public Events Do(Attack command)
        {
            yield return new SpellCast(command.TargetId, "Shield");
            yield return new SpellCast(command.TargetId, "Fireball");

            if (ExperienceLevel > 2 && command.Level == Attack.AttackLevel.Kill)
            {
                yield return new SpellCast(command.TargetId, "Invisibilty");
                yield return new SpellCast(command.TargetId, "Teleportation");
                yield return new SpellCast(command.TargetId, "Death Touch");
            }
        }
    }
}