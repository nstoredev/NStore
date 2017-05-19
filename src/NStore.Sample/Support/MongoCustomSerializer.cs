using System;
using System.Reflection;
using Newtonsoft.Json;
using NStore.Aggregates;
using NStore.Persistence.Mongo;

namespace NStore.Sample.Support
{
    public class MongoCustomSerializer : ISerializer
    {
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        public object Serialize(string partitionId, object payload)
        {
            if (payload == null) return null;

            var json = JsonConvert.SerializeObject(payload, _settings);
            if (partitionId.EndsWith("3"))
            {
                return System.Text.Encoding.UTF8.GetBytes(json);
            }

            return json;
        }

        public object Deserialize(string partitionId, object payload)
        {
            if (payload == null) return null;

            if (payload is byte[] ba)
            {
                payload = System.Text.Encoding.UTF8.GetString(ba);
            }
            return JsonConvert.DeserializeObject((string)payload, _settings);
        }
    }
}