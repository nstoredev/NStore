namespace NStore.LoadTests
{
    public sealed class DeviceMessage
    {
        public string DeviceId { get; set; }
        public long Sequence { get; set; }
        public long Counter1  { get; set; }
        public long Counter2  { get; set; }
        public long Counter3  { get; set; }
        public long Counter4  { get; set; }
    }
}