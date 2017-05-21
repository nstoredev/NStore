using System;
using System.Reflection;
using Newtonsoft.Json;
using NStore.Persistence.Mongo;

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

    public class MongoCustomSerializer : ISerializer
    {
        public object Serialize(string partitionId, object payload)
        {
            if (payload == null) return null;

            var json = ObjectSerializer.Serialize(payload);
            if (partitionId.EndsWith("2"))
            {
                // no conversion => BSON
                return payload;
            }
            else if (partitionId.EndsWith("3"))
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

            if (payload is string sp)
            {
                return ObjectSerializer.Deserialize(sp);
            }

            return payload;
        }
    }
}