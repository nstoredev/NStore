using Newtonsoft.Json;

namespace NStore.Persistence.MsSql.Tests
{
    public class JsonMsSqlSerializer : IMsSqlPayloadSearializer
    {
        JsonSerializerSettings Settings { get; set; }

        public JsonMsSqlSerializer()
        {
            this.Settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            };
        }

        public object Deserialize(string serialized)
        {
            return JsonConvert.DeserializeObject(serialized, (JsonSerializerSettings) Settings);
        }

        public string Serialize(object payload)
        {
            return JsonConvert.SerializeObject(payload, (JsonSerializerSettings) Settings);
        }
    }
}