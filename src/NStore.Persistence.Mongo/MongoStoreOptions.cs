using System;

namespace NStore.Persistence.Mongo
{
    public class MongoStoreOptions
    {
        public string PartitionsConnectionString { get; set; }
        public string PartitionsCollectionName { get; set; } = "chunks";

        public string SequenceConnectionString { get; set; }
        public string SequenceCollectionName { get; set; } = "seq";
        public string SequenceId { get; set; } = "streams";
        public bool UseLocalSequence { get; set; } = false;
        public bool DropOnInit { get; set; } = false;

        public ISerializer Serializer { get; set; }

        public bool IsValid()
        {
            return !String.IsNullOrWhiteSpace(PartitionsConnectionString);
        }
    }
}