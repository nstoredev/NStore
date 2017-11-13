using System.Text;
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

        public object Deserialize(byte[] serialized)
        {
            var json = Encoding.UTF8.GetString(serialized);
            return JsonConvert.DeserializeObject(json, Settings);
        }

        public byte[] Serialize(object payload)
        {
            var json = JsonConvert.SerializeObject(payload, Settings);
            return Encoding.UTF8.GetBytes(json);

        }
    }
}