using NStore.Core.Processing;

namespace NStore.Domain.Poco
{
    public class DefaultCommandProcessor : ICommandProcessor
    {
        public static readonly ICommandProcessor Instance = new DefaultCommandProcessor();

        private DefaultCommandProcessor()
        {
        }

        public object RunCommand(object state, object command)
        {
            return state.CallPublic("Do", command);
        }
    }
}