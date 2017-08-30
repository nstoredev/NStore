using Newtonsoft.Json;

namespace NStore.Persistence.Sqlite.Tests
{
    public class JsonMsSqlSerializer : ISqlitePayloadSearializer
    {
        JsonSerializerSettings Settings { get; set; }

        public JsonMsSqlSerializer()
        {
            this.Settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All,
                Formatting = Formatting.Indented
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