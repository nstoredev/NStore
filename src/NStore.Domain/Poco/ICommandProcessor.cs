namespace NStore.Domain.Poco
{
    public interface ICommandProcessor
    {
        object RunCommand(object state, object command);
    }
}