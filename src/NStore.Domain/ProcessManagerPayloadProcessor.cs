using NStore.Core.Processing;

namespace NStore.Domain
{
    public sealed class ProcessManagerPayloadProcessor : IPayloadProcessor
    {
        public static readonly ProcessManagerPayloadProcessor Instance = new ProcessManagerPayloadProcessor();

        private readonly string[] _methods = { "On", "StartedBy", "ContinuedBy", "CompletedBy" };

        public object Process(object state, object payload)
        {
            return state.CallNonPublicIfExists(_methods, payload);
        }
    }
}