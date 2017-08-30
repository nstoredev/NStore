using NStore.Core.Logging;

namespace NStore.Persistence.Sqlite
{
    public interface ISqlitePayloadSearializer
    {
        string Serialize(object payload);
        object Deserialize(string serialized);
    }
    
    public class SqlitePersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }
        public ISqlitePayloadSearializer Serializer { get; set; }
        public string ConnectionString { get; set; }
        public string StreamsTableName { get; set; }

        public SqlitePersistenceOptions(INStoreLoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            StreamsTableName = "Streams";
        }

        public virtual string GetCreateTableScript(string streamsTableName)
        {
            return $@"CREATE TABLE [{streamsTableName}](
                [Position] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                [PartitionId] NVARCHAR(255) NOT NULL,
                [OperationId] NVARCHAR(255) NOT NULL,
                [Index] BIGINT NOT NULL,
                [Deleted] BIT NOT NULL,
                [Payload] NVARCHAR(10000)
            );

            CREATE UNIQUE INDEX IX_{streamsTableName}_OPID on {streamsTableName} (PartitionId, OperationId);
            CREATE UNIQUE INDEX IX_{streamsTableName}_IDX on {streamsTableName} (PartitionId, [Index]);
";
        }

        public virtual string GetPersistScript(string streamsTableName)
        {
            return $@"INSERT INTO [{streamsTableName}]
                      ([PartitionId], [Index], [Payload], [OperationId], [Deleted])
                      VALUES (@PartitionId, @Index, @Payload, @OperationId, 0);

                      SELECT last_insert_rowid();
";
        }

        public virtual string GetDeleteStreamScript()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
        }

        public virtual string GetLastChunkScript()
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [Deleted]
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] <= @toUpperIndexInclusive 
                      ORDER BY 
                          [Position] DESC
                      LIMIT 1";
        }
    }
}