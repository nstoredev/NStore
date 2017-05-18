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
        
        public object Serialize(object input)
        {
            if (input == null) return null;
            return JsonConvert.SerializeObject(input, _settings);
        }

        public object Deserialize(object input)
        {
            if (input == null) return null;

            return JsonConvert.DeserializeObject((string)input,_settings);
        }
    }
}