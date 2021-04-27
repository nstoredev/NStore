namespace NStore.Sample.Support
{
    public class NullReporter : IReporter
    {
        public static readonly IReporter Instance = new NullReporter();

        private NullReporter()
        {
            
        }

        public void Report(string message)
        {
            // nothing to do here...
        }
    }
}
