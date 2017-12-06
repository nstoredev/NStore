namespace NStore.Domain.Tests.PocoAggregateTests
{
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
}