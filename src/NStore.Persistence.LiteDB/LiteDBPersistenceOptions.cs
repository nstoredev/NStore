using System;
using LiteDB;
using NStore.Core.Logging;

namespace NStore.Persistence.LiteDB
{
    public class LiteDBPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; }
        public ILiteDBPayloadSerializer PayloadSerializer { get; }

        public LiteDBPersistenceOptions(
            ILiteDBPayloadSerializer serializer,
            INStoreLoggerFactory loggerFactory,
            BsonMapper mapper = null
        )
        {
            PayloadSerializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            LoggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.Mapper = mapper;
        }

        public string ConnectionString { get; set; }
        public string StreamsCollectionName { get; set; }
        public BsonMapper Mapper { get; set; }
    }
}