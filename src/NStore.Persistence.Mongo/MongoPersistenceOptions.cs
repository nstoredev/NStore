using MongoDB.Driver;
using NStore.Core.Logging;
using System;

namespace NStore.Persistence.Mongo
{
    public class MongoPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }

        public string PartitionsConnectionString { get; set; }
        public string PartitionsCollectionName { get; set; } = "chunks";

        /// <summary>
        /// ConnectionString that points to database to use to generate sequences
        /// if we want to use sequences generated in database.
        /// </summary>
        public string SequenceConnectionString { get; set; }

        /// <summary>
        /// Collection name to use to store generated sequences.
        /// </summary>
        public string SequenceCollectionName { get; set; } = "seq";
        public string SequenceId { get; set; } = "streams";
        public bool UseLocalSequence { get; set; } = false;
        internal bool DropOnInit { get; set; } = false;

        public IMongoPayloadSerializer MongoPayloadSerializer { get; set; }

        /// <summary>
        /// This allows customization of the connection string for Partition Data
        /// </summary>
        public Action<MongoUrlBuilder> CustomizePartitionSettings { get; set; }

        /// <summary>
        /// This allows the customization of MongoClientSettings used to create mongo client
        /// for the Partition Database
        /// </summary>
        public Action<MongoClientSettings> CustomizePartitionClientSettings { get; set; }

        /// <summary><
        /// This allows customization of the connection string for Sequence Data.
        /// </summary>
        public Action<MongoUrlBuilder> CustomizeSequenceSettings { get; set; }

        /// <summary>
        /// This allows the customization of MongoClientSettings used to create mongo client
        /// for the Sequence Database
        /// </summary>
        public Action<MongoClientSettings> CustomizeSequenceClientSettings { get; set; }

        /// <summary>
        /// This function allows callers to override the <see cref="IMongoClient"/> creation
        /// function to avoid creating too much IMongoClient.
        /// </summary>
        /// <remarks>
        /// IMPORTANT: MongoClient is thread-safe and maintains an internal connection pool.
        /// It is recommended to use a singleton instance or cache MongoClient instances
        /// per connection string to maximize connection pooling efficiency.
        /// Creating multiple MongoClient instances creates separate connection pools,
        /// which increases resource usage and reduces performance.
        /// 
        /// Example of singleton pattern:
        /// <code>
        /// private static readonly ConcurrentDictionary&lt;string, IMongoClient&gt; _clientCache = new();
        /// options.CreateClientFunction = settings => 
        ///     _clientCache.GetOrAdd(settings.ToString(), _ => new MongoClient(settings));
        /// </code>
        /// </remarks>
        public Func<MongoClientSettings, IMongoClient> CreateClientFunction { get; set; } = settings => new MongoClient(settings);

        /// <summary>
        /// If we have a readonly user we cannot perform any update on the database so we need not
        /// to create index and do other write/support routine. This WILL NOT PREVENT WRITING TO
        /// THE STREAM IF THE USER USED TO CONNECT TO MONGO HAS WRITE PERMISSION
        /// </summary>
        public bool ReadonlyUser { get; set; }

        /// <summary>
        /// The number of documents MongoDB returns in each batch when reading data.
        /// Default is null (uses MongoDB driver default of ~100 documents).
        /// </summary>
        /// <remarks>
        /// <para>
        /// Tuning this value can significantly impact performance:
        /// - Higher values (500-1000): Fewer network round-trips, better throughput for bulk reads,
        ///   but higher memory usage per cursor. Good for reading large event streams.
        /// - Lower values (50-100): Lower memory footprint, faster initial response,
        ///   better for pagination or reading small chunks. Good for UI scenarios.
        /// - Very high values (>2000): May hit MongoDB's 16MB message size limit.
        /// </para>
        /// <para>
        /// Monitor your workload: If you see many small batches in logs/profiling,
        /// increase this value. If memory pressure is high, decrease it.
        /// </para>
        /// </remarks>
        public int? CursorBatchSize { get; set; } = null;

        /// <summary>
        /// Maximum number of retry attempts for batch append operations when encountering recoverable errors.
        /// Default is 100. If this limit is exceeded, a <see cref="BatchRetryLimitExceededException"/> is thrown.
        /// </summary>
        /// <remarks>
        /// Reaching this limit typically indicates a systemic problem such as:
        /// - Sequence generator failure
        /// - Extreme contention from multiple processes
        /// - Database connectivity issues
        /// Consider increasing this value only if you have verified the underlying cause.
        /// </remarks>
        public int BatchAppendMaxRetries { get; set; } = 100;

        /// <summary>
        /// Specify to the persistence layer that we have a connection string with a readonly user
        /// so we cannot try to create indexes on initialization or else we are not able to create a 
        /// persistence to read data.
        /// </summary>
        /// <returns></returns>
        public MongoPersistenceOptions SetReadonlyUser()
        {
            ReadonlyUser = true;
            return this;
        }

        public MongoPersistenceOptions SetDropOnInit()
        {
            DropOnInit = true;
            return this;
        }

        public bool IsValid()
        {
            return !String.IsNullOrWhiteSpace(PartitionsConnectionString);
        }

        public MongoPersistenceOptions()
        {
#pragma warning disable RCS1163 // Unused parameter.
            this.CustomizePartitionSettings = settings => { };
            this.CustomizeSequenceSettings = settings => { };

            this.CustomizePartitionClientSettings = clientSettings => { };
            this.CustomizeSequenceClientSettings = clientSettings => { };
#pragma warning restore RCS1163 // Unused parameter.
            LoggerFactory = NStoreNullLoggerFactory.Instance;
        }
    }
}