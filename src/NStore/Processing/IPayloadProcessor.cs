namespace NStore.Processing
{
    public interface IPayloadProcessor
    {
        void Process(object payload);
    }
}