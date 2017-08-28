namespace NStore.Core.Processing
{
    public sealed class DelegateToPublicEventHandlers : IPayloadProcessor
    {
        public static readonly IPayloadProcessor Instance = new DelegateToPublicEventHandlers();

        private DelegateToPublicEventHandlers()
        {
        }

        public object Process(object state, object payload)
        {
            return state.CallPublicIfExists("On", payload);
        }
    }
}