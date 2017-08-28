using NStore.Processing;

namespace NStore.Processing
{
    public sealed class DelegateToPrivateEventHandlers : IPayloadProcessor
    {
        public static readonly IPayloadProcessor Instance = new DelegateToPrivateEventHandlers();

        private DelegateToPrivateEventHandlers()
        {
        }

        public object Process(object state, object payload)
        {
            return state.CallNonPublicIfExists("On", payload);
        }
    }
}