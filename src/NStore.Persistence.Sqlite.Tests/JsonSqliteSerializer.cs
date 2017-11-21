using System.Text;
using Newtonsoft.Json;

namespace NStore.Persistence.Sqlite.Tests
{
    public class JsonSqliteSerializer : ISqlitePayloadSerializer
    {
        JsonSerializerSettings Settings { get; set; }

        public JsonSqliteSerializer()
        {
            this.Settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All
            };
        }

        public object Deserialize(byte[] serialized, string serializerInfo)
        {
            if (serializerInfo == "byte[]")
                return serialized;

            var json = Encoding.UTF8.GetString(serialized);
            return JsonConvert.DeserializeObject(json, Settings);
        }

        public byte[] Serialize(object payload, out string serializerInfo)
        {
            if (payload is byte[] bytes)
            {
                serializerInfo = "byte[]";
                return bytes;
            }

            serializerInfo = "json";
            var json = JsonConvert.SerializeObject(payload, Settings);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}