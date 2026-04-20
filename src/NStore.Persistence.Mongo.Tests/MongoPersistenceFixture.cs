using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using Xunit;
using Xunit.Sdk;

#if MAP_DOMAIN
using MongoDB.Bson.Serialization.Options;
using NStore.Domain;
#endif

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        private const string MongoConnectionEnvVar = "NSTORE_MONGODB";
        private const string PerfEnabledConfigKey = "NStore:Mongo:Performance:Enabled";
        private static readonly string[] PerfMongoConnectionConfigKeys =
        {
            "NStore:Mongo:Performance:ConnectionString"
        };

        private static readonly string[] MongoConnectionConfigKeys =
        {
            "NStore:Mongo:ConnectionString"
        };

        protected static readonly Lazy<IConfigurationRoot> TestConfiguration =
            new Lazy<IConfigurationRoot>(BuildConfiguration);

        protected string _mongoConnectionString;
        protected IMongoPersistence _mongoPersistence;
        private MongoPersistenceOptions _options;
        private string _mongoStoreScope;
        private int _mongoStoreScopeVersion;
        private const string TestSuitePrefix = "Mongo";

        static BasePersistenceTest()
        {
            // https://github.com/mongodb/mongo-csharp-driver/releases/tag/v2.19.0 
            var objectSerializer = new ObjectSerializer(type => ObjectSerializer.AllAllowedTypes(type));
            BsonSerializer.RegisterSerializer(objectSerializer);

#if MAP_DOMAIN
            // enable support for dots in key names
            if (!BsonClassMap.IsClassMapRegistered(typeof(Changeset)))
            {
                BsonClassMap.RegisterClassMap<Changeset>(map =>
                {
                    map.AutoMap();
                    map.MapProperty(x => x.Headers).SetSerializer(
                        new DictionaryInterfaceImplementerSerializer<
                            Dictionary<string, object>
                        >(DictionaryRepresentation.ArrayOfArrays)
                    );
                });
            }
#endif
        }

        protected internal IPersistence Create(bool dropOnInit)
        {
            try
            {
                _mongoConnectionString = GetPartitionsConnectionString();
                if (dropOnInit || string.IsNullOrWhiteSpace(_mongoStoreScope))
                {
                    _mongoStoreScopeVersion++;
                    _mongoStoreScope = _testRunId + "_" + Environment.Version.Major + "_" + _mongoStoreScopeVersion;
                }

                _options = GetMongoPersistenceOptions();
                ApplyMongoStoreScope(_options, _mongoStoreScope);
                if (dropOnInit)
                {
                    _options.SetDropOnInit();
                }
                _mongoPersistence = CreatePersistence(_options);

                _mongoPersistence.InitAsync(CancellationToken.None).GetAwaiter().GetResult();

                return _mongoPersistence;
            }
            catch (TestMisconfiguredException ex)
            {
                throw SkipException.ForSkip("Mongo integration tests skipped: " + ex.Message);
            }
            catch (MongoAuthenticationException ex)
            {
                throw SkipException.ForSkip("Mongo integration tests skipped: " + ex.Message);
            }
            catch (MongoCommandException ex) when (RequiresAuthenticatedMongoConnection(ex))
            {
                throw SkipException.ForSkip("Mongo integration tests skipped: " + ex.Message);
            }
        }

        private static void ApplyMongoStoreScope(MongoPersistenceOptions options, string storeScope)
        {
            options.PartitionsCollectionName = options.PartitionsCollectionName + "_" + storeScope;
            options.SequenceCollectionName = options.SequenceCollectionName + "_" + storeScope;
        }

        private static bool RequiresAuthenticatedMongoConnection(MongoCommandException ex)
        {
            return ex.Message.IndexOf("requires authentication", StringComparison.OrdinalIgnoreCase) >= 0 ||
                   ex.Message.IndexOf("not authorized", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        protected virtual internal MongoPersistenceOptions GetMongoPersistenceOptions()
        {
            if (string.IsNullOrWhiteSpace(_mongoConnectionString))
            {
                _mongoConnectionString = GetPartitionsConnectionString();
            }

            return new MongoPersistenceOptions
            {
                PartitionsConnectionString = _mongoConnectionString,
                UseLocalSequence = true,
                PartitionsCollectionName = "partitions_" + GetType().Name + "_" + _testRunId,
                SequenceCollectionName = "seq_" + _testRunId
            };
        }

        private static string GetPartitionsConnectionString()
        {
            var config = TestConfiguration.Value;
            if (ReadBooleanSetting(config, PerfEnabledConfigKey, fallback: false))
            {
                var perfMongo = ReadFirstConfiguredValue(config, PerfMongoConnectionConfigKeys);
                if (!string.IsNullOrWhiteSpace(perfMongo))
                {
                    return perfMongo;
                }

                var defaultMongo = ReadFirstConfiguredValue(config, MongoConnectionConfigKeys);
                if (!string.IsNullOrWhiteSpace(defaultMongo))
                {
                    return defaultMongo;
                }

                throw new TestMisconfiguredException(
                    $"Mongo connection string not set. Configure appsettings/user-secrets keys: {string.Join(", ", PerfMongoConnectionConfigKeys)} (recommended for perf mode) or {string.Join(", ", MongoConnectionConfigKeys)}.");
            }

            var mongo = Environment.GetEnvironmentVariable(MongoConnectionEnvVar);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            mongo = ReadFirstConfiguredValue(config, MongoConnectionConfigKeys);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            throw new TestMisconfiguredException(
                $"Mongo connection string not set. Configure {MongoConnectionEnvVar} or appsettings/user-secrets key: {string.Join(", ", MongoConnectionConfigKeys)}.");
        }

        private static string ReadFirstConfiguredValue(IConfiguration config, string[] keys)
        {
            for (var i = 0; i < keys.Length; i++)
            {
                var value = config[keys[i]];
                if (!string.IsNullOrWhiteSpace(value))
                {
                    return value;
                }
            }

            return null;
        }

        private static bool IsEnabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "1" ||
                   value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("yes", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsDisabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "0" ||
                   value.Equals("false", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("no", StringComparison.OrdinalIgnoreCase);
        }

        private static bool ReadBooleanSetting(IConfiguration config, string key, bool fallback)
        {
            var raw = config[key];
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (IsEnabled(raw))
            {
                return true;
            }

            if (IsDisabled(raw))
            {
                return false;
            }

            throw new ArgumentException(
                $"{key} must be a boolean value (supported: true/false, yes/no, 1/0), but was '{raw}'.");
        }

        private static string FindAppSettingsBasePath()
        {
            var current = new DirectoryInfo(AppContext.BaseDirectory);
            while (current != null)
            {
                var appsettingsPath = Path.Combine(current.FullName, "appsettings.json");
                if (File.Exists(appsettingsPath))
                {
                    return current.FullName;
                }

                current = current.Parent;
            }

            return AppContext.BaseDirectory;
        }

        private static IConfigurationRoot BuildConfiguration()
        {
            var basePath = FindAppSettingsBasePath();
            return new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
                .AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false)
                .AddUserSecrets(typeof(BasePersistenceTest).Assembly, optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        protected static IConfigurationRoot GetTestConfiguration()
        {
            return TestConfiguration.Value;
        }

        protected IMongoCollection<TChunk> GetCollection<TChunk>()
        {
            var fieldInfo = _mongoPersistence.GetType()
                .GetField("_chunks", BindingFlags.NonPublic | BindingFlags.Instance);

            var collection = (IMongoCollection<TChunk>)fieldInfo.GetValue(_mongoPersistence);
            return collection;
        }

        protected virtual IMongoPersistence CreatePersistence(MongoPersistenceOptions options)
        {
            return new MongoPersistence(options);
        }

        protected void Clear(IPersistence persistence, bool drop)
        {
            if (!drop)
            {
                return;
            }

            // Each Create() with dropOnInit or an unset scope bumps the scope version so
            // collections are uniquely named per test. Without this cleanup, every run
            // would leak scoped collections on local MongoDB instances.
            if (persistence is IMongoPersistence mongo)
            {
                try
                {
                    mongo.DropAsync(CancellationToken.None).GetAwaiter().GetResult();
                }
                catch (MongoAuthenticationException)
                {
                    // Integration test environment without write access - skip cleanup.
                }
                catch (MongoCommandException ex) when (RequiresAuthenticatedMongoConnection(ex))
                {
                    // Integration test environment without write access - skip cleanup.
                }
            }
        }
    }
}
