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
        public bool DropOnInit { get; set; } = false;

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
        /// If we have a readonly user we cannot perform any update on the database so we need not
        /// to create index and do other write/support routine. This WILL NOT PREVENT WRITING TO
        /// THE STREAM IF THE USER USED TO CONNECT TO MONGO HAS WRITE PERMISSION
        /// </summary>
        public bool ReadonlyUser { get; set; }

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