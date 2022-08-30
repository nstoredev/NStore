using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using NStore.BaseSqlPersistence;
using NStore.Core.Logging;

namespace NStore.Persistence.Sqlite
{
    public class SqlitePersistenceOptions : BaseSqlPersistenceOptions
    {
        public SqlitePersistenceOptions(INStoreLoggerFactory loggerFactory) : base(loggerFactory)
        {
        }

        protected virtual string GetCreateTableSql()
        {
            return $@"CREATE TABLE [{StreamsTableName}](
                [Position] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                [PartitionId] NVARCHAR(255) NOT NULL,
                [Index] BIGINT NOT NULL,
                [OperationId] NVARCHAR(255) NOT NULL,
                [SerializerInfo] NVARCHAR(255) NOT NULL,
                [Payload] BLOB
            );

            CREATE UNIQUE INDEX IX_{StreamsTableName}_OPID on {StreamsTableName} (PartitionId, OperationId);
            CREATE UNIQUE INDEX IX_{StreamsTableName}_IDX on {StreamsTableName} (PartitionId, [Index]);
";
        }

        public override string GetInsertChunkSql()
        {
            return $@"INSERT INTO [{StreamsTableName}]
                      ([PartitionId], [Index], [Payload], [OperationId], [SerializerInfo])
                      VALUES (@PartitionId, @Index, @Payload, @OperationId, @SerializerInfo);

                      SELECT last_insert_rowid();
";
        }
        
        public override string GetReplaceChunkSql()
        {
            return $@"UPDATE [{StreamsTableName}]
                    SET [PartitionId] = @PartitionId,
                        [Index] = @Index,
                        [Payload] = @Payload,
                        [OperationId] = @OperationId,
                        [SerializerInfo] = @SerializerInfo
                    WHERE [Position] = @Position";
        }

        public override string GetSelectChunkByStreamAndOperation()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM [{StreamsTableName}] 
                      WHERE [PartitionId] = @PartitionId AND [OperationId] = @OperationId";
        }

        public override string GetSelectAllChunksByOperationSql()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM [{StreamsTableName}] 
                      WHERE [OperationId] = @OperationId";
        }

        public override string GetDeleteStreamChunksSql()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
        }

        public override string GetSelectLastChunkSql()
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] 
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] <= @toUpperIndexInclusive 
                      ORDER BY 
                          [Position] DESC
                      LIMIT 1";
        }

        public override string GetChunkByPositionSql()
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM 
                        [{this.StreamsTableName}] 
                      WHERE 
                          [Position] = @Position";
        }
        
        public override string GetCreateTableIfMissingSql()
        {
            return GetCreateTableSql();
        }

        public override string GetReadAllChunksSql(int limit)
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] 
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [Position] >= @fromPositionInclusive 
                      ORDER BY 
                          [Position] LIMIT {limit}";
        }

        public override string GetRangeSelectChunksSql(long upperIndexInclusive, long lowerIndexInclusive, int limit, bool descending)
        {
            var sb = new StringBuilder("SELECT ");
            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
            sb.Append($"FROM {StreamsTableName} ");
            sb.Append("WHERE [PartitionId] = @PartitionId ");

            if (lowerIndexInclusive > 0 && lowerIndexInclusive != Int64.MinValue)
            {
                sb.Append("AND [Index] >= @lowerIndexInclusive ");
            }

            if (upperIndexInclusive > 0 && upperIndexInclusive != Int64.MaxValue)
            {
                sb.Append("AND [Index] <= @upperIndexInclusive ");
            }

            sb.Append(descending ? "ORDER BY [Index] DESC" : "ORDER BY [Index]");

            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($"LIMIT {limit} ");
            }

            return sb.ToString();
        }

        public override async Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken)
        {
            var connection =  new SqliteConnection(ConnectionString);
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception )
            {
                connection.Dispose();
                throw;
            }
            return new SqliteContext(connection, true);
        }

        public override string GetSelectLastPositionSql()
        {
            return $@"SELECT 
                        [Position]
                      FROM 
                        [{StreamsTableName}] 
                      ORDER BY 
                          [Position] DESC
                      LIMIT 1";
        }
    }
}