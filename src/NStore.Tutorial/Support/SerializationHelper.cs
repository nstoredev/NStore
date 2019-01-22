using Newtonsoft.Json;

namespace NStore.Tutorial.Support
{
    public static class SerializationHelper
    {
        public static T DeepClone<T>(T source)
        {
            // * * * * * * * * * * * * * * * * * * * * * * *
            //                 DISCLAIMER                 //
            // * * * * * * * * * * * * * * * * * * * * * * *
            //
            // Just a sample, quick & dirty for the tutorial.
            // Don't do it for real
            //
            // * * * * * * * * * * * * * * * * * * * * * * *
            return JsonConvert.DeserializeObject<T>(
                JsonConvert.SerializeObject(source)
            );
        }
    }
}