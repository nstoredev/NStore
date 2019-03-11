using System.Text;
using Newtonsoft.Json;
using NStore.Persistence.MsSql;

namespace NStore.Tutorial.Support
{
    internal class JsonMsSqlSerializer : IMsSqlPayloadSerializer
    {
        JsonSerializerSettings Settings { get; set; }

        public JsonMsSqlSerializer()
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