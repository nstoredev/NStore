using Newtonsoft.Json;

namespace NStore.Sample.Support
{
    public static class ObjectSerializer
    {
        private static readonly JsonSerializerSettings _settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        public static string Serialize(object payload)
        {
            return JsonConvert.SerializeObject(payload, _settings);
        }

        public static object Deserialize(string payload)
        {
            return JsonConvert.DeserializeObject(payload, _settings);
        }

        public static object Clone(object payload)
        {
            return Deserialize(Serialize(payload));
        }
    }
}