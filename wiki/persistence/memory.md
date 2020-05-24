# Memory Persistence

### Description
```InMemoryPersistence``` is a persistence layer suitable for testing without a database instance.

### Setup
```csharp
var persistence = new InMemoryPersistence( SerializationHelper.DeepClone );
```
where `SerializationHelper.DeepClone` is a custom function for simulating serialization.

```csharp
public static class SerializationHelper
{
    class TutorialContractResolver : DefaultContractResolver
    {
        protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
        {
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .Select(f => base.CreateProperty(f, memberSerialization))
                .Select(p =>
                {
                    p.Readable = true;
                    p.Writable = true;
                    return p;
                });

            var props = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                            BindingFlags.SetProperty)
                    .Select(p => base.CreateProperty(p, memberSerialization))
                    .Select(p =>
                    {
                        p.Readable = true;
                        return p;
                    })
                ;

            return props.Union(fields).ToList();
        }
    }

    private static readonly JsonSerializerSettings Settings;

    static SerializationHelper()
    {
        Settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            ContractResolver = new TutorialContractResolver()
        };
    }

    public static T DeepClone<T>(T source)
    {
        // Only for tests 
        return JsonConvert.DeserializeObject<T>(
            JsonConvert.SerializeObject(source, Settings),
            Settings
        );
    }

    public static string ToJson(object source)
    {
        return JsonConvert.SerializeObject(source, Formatting.Indented);
    }
}
```

### ToDo
Allow explicit serialization / deserialization to test serializers.
