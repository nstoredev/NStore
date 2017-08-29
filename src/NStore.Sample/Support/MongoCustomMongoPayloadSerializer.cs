using System;
using System.Collections.Generic;
using System.Reflection;
using NStore.Persistence.Mongo;

namespace NStore.Sample.Support
{
    public class DiagnosticPayloadWrapper
    {
        public Dictionary<string, string> Context { get; private set; }
        public object Payload { get; private set; }
        public DateTime TimeStamp { get; private set; }

        public DiagnosticPayloadWrapper(object payload)
        {
            Context = new Dictionary<string, string>();
            Payload = payload;
            TimeStamp = DateTime.UtcNow;

            Context["os-platform"] = Environment.OSVersion.Platform.ToString();
            Context["os-version"] = Environment.OSVersion.VersionString;
            Context["app-version"] = Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            Context["machine-name"] = Environment.MachineName;
        }
    }

    public class DiagnosticSerializerWrapper : IMongoPayloadSerializer
    {
        private readonly IMongoPayloadSerializer _serializer;

        public DiagnosticSerializerWrapper(IMongoPayloadSerializer serializer)
        {
            _serializer = serializer;
        }

        public object Serialize(object payload)
        {
            return new DiagnosticPayloadWrapper(_serializer.Serialize(payload));
        }

        public object Deserialize(object payload)
        {
            if (payload is DiagnosticPayloadWrapper dpw)
            {
                return dpw.Payload;
            }
            return payload;
        }
    }

    public class MongoCustomMongoPayloadSerializer : IMongoPayloadSerializer
    {
        private int _counter;
        public object Serialize(object payload)
        {
            if (payload == null) return null;

            _counter++;

            if (_counter % 3 == 0)
            {
                // no conversion => BSON
                return payload;
            }

            var json = ObjectSerializer.Serialize(payload);

            if (_counter % 5 == 0)
            {
                return System.Text.Encoding.UTF8.GetBytes(json);
            }

            return json;
        }

        public object Deserialize(object payload)
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