using NStore.Core.Logging;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }
        public ISqlitePayloadSerializer Serializer { get; set; }
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
                [Payload] NVARCHAR(10000)
            );

            CREATE UNIQUE INDEX IX_{streamsTableName}_OPID on {streamsTableName} (PartitionId, OperationId);
            CREATE UNIQUE INDEX IX_{streamsTableName}_IDX on {streamsTableName} (PartitionId, [Index]);
";
        }

        public virtual string GetPersistScript(string streamsTableName)
        {
            return $@"INSERT INTO [{streamsTableName}]
                      ([PartitionId], [Index], [Payload], [OperationId])
                      VALUES (@PartitionId, @Index, @Payload, @OperationId);

                      SELECT last_insert_rowid();
";
        }

        public virtual string GetFindByStreamAndOperation()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId]
                      FROM [{StreamsTableName}] 
                      WHERE [PartitionId] = @PartitionId AND [OperationId] = @OperationId";
        }

        public virtual string GetFindAllByOperation()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId]
                      FROM [{StreamsTableName}] 
                      WHERE [OperationId] = @OperationId";
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
                        [Position], [PartitionId], [Index], [Payload], [OperationId]
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