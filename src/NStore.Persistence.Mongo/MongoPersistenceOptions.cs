using System;
using System.Collections.Generic;
using MongoDB.Driver;

namespace NStore.Persistence.Mongo
{
    public class MongoPersistenceOptions
    {
        public string PartitionsConnectionString { get; set; }
        public string PartitionsCollectionName { get; set; } = "chunks";

        public string SequenceConnectionString { get; set; }
        public string SequenceCollectionName { get; set; } = "seq";
        public string SequenceId { get; set; } = "streams";
        public bool UseLocalSequence { get; set; } = false;
        public bool DropOnInit { get; set; } = false;

        public ISerializer Serializer { get; set; }

        public Action<MongoUrlBuilder> CustomizePartitionSettings { get; set; }
        public Action<MongoUrlBuilder> CustomizeSquenceSettings { get; set; }

        public bool IsValid()
        {
            return !String.IsNullOrWhiteSpace(PartitionsConnectionString);
        }

        public MongoPersistenceOptions()
        {
            this.CustomizePartitionSettings = settings => { };
            this.CustomizeSquenceSettings = settings => { };
        }
    }
}