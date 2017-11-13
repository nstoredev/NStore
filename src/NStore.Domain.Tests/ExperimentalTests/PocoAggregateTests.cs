using System.Collections.Generic;
using NStore.Core.InMemory;
using NStore.Core.Snapshots;
using NStore.Domain.Experimental;
using Xunit;
// ReSharper disable ClassNeverInstantiated.Global
using Events = System.Collections.Generic.IEnumerable<object>;

namespace NStore.Domain.Tests.ExperimentalTests
{
    public class SpellCasted
    {
        public SpellCasted(string targetId, string name)
        {
            TargetId = targetId;
            Name = name;
        }

        public string TargetId { get; private set; }
        public string Name { get; private set; }
    }

    public class Attack
    {
        public enum AttackLevel
        {
            Hit,
            Kill,
            Armageddon
        }

        public string TargetId { get; private set; }
        public AttackLevel Level { get; private set; }

        public Attack(string targetId, AttackLevel level)
        {
            TargetId = targetId;
            Level = level;
        }
    }


    public class Mage
    {
        private int CastedSpellCount { get; set; }
        public double ExperienceLevel => CastedSpellCount / 20.0;

        private void On(SpellCasted e)
        {
            this.CastedSpellCount++;
        }

        public Events Do(Attack command)
        {
            yield return new SpellCasted(command.TargetId, "Shield");
            yield return new SpellCasted(command.TargetId, "Fireball");

            if (ExperienceLevel > 2 && command.Level == Attack.AttackLevel.Kill)
            {
                yield return new SpellCasted(command.TargetId, "Invisibilty");
                yield return new SpellCasted(command.TargetId, "Teleportation");
                yield return new SpellCasted(command.TargetId, "Death Touch");
            }
        }
    }
}