namespace NStore.Processing
{
    public interface IPayloadProcessor
    {
        object Process(object state, object payload);
    }
}