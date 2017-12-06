namespace NStore.Domain.Tests.PocoAggregateTests
{
    public class SpellCast
    {
        public SpellCast(string targetId, string name)
        {
            TargetId = targetId;
            Name = name;
        }

        public string TargetId { get; private set; }
        public string Name { get; private set; }
    }
}