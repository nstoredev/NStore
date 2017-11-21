using System;
using System.Text;
using NStore.BaseSqlPersistence;
using NStore.Core.Logging;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistenceOptions : BaseSqlPersistenceOptions
    {
        public MsSqlPersistenceOptions(INStoreLoggerFactory loggerFactory) : base(loggerFactory)
        {
        }

        protected virtual string GetCreateTableSql()
        {
            return $@"CREATE TABLE [{StreamsTableName}](
                [Position] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                [PartitionId] NVARCHAR(255) NOT NULL,
                [OperationId] NVARCHAR(255) NOT NULL,
                [SerializerInfo] NVARCHAR(255) NOT NULL,
                [Index] BIGINT NOT NULL,
                [Payload] VARBINARY(MAX)
            )

            CREATE UNIQUE INDEX IX_{StreamsTableName}_OPID on dbo.{StreamsTableName} (PartitionId, OperationId)
            CREATE UNIQUE INDEX IX_{StreamsTableName}_IDX on dbo.{StreamsTableName} (PartitionId, [Index])
";
        }

        public override string GetInsertChunkSql()
        {
            return $@"INSERT INTO [{StreamsTableName}]
                      ([PartitionId], [Index], [Payload], [OperationId], [SerializerInfo])
                      OUTPUT INSERTED.[Position] 
                      VALUES (@PartitionId, @Index, @Payload, @OperationId, @SerializerInfo)";
        }

        public override string GetDeleteStreamChunksSql()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
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

        public override string GetSelectLastChunkSql()
        {
            return $@"SELECT TOP 1 
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] <= @toUpperIndexInclusive 
                      ORDER BY 
                          [Position] DESC";
        }

        public override string GetRangeSelectChunksSql(
            long upperIndexInclusive,
            long lowerIndexInclusive,
            int limit,
            bool descending = true
        )
        {
            var sb = new StringBuilder("SELECT ");
            if (limit > 0 && limit != int.MaxValue)
            {
                sb.Append($"TOP {limit} ");
            }

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

            sb.Append(@descending ? "ORDER BY [Index] DESC" : "ORDER BY [Index]");

            return sb.ToString();
        }

        public virtual string GetDropTableSql()
        {
            return
                $"if exists (select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '{this.StreamsTableName}' AND TABLE_SCHEMA = 'dbo') " +
                $"DROP TABLE {this.StreamsTableName}";
        }

        public override string GetCreateTableIfMissingSql()
        {
            var sql = GetCreateTableSql();
            
            return $@"
if not exists (select * from dbo.sysobjects where id = object_id(N'{{StreamsTableName}}') and OBJECTPROPERTY(id, N'IsUserTable') = 1) 
BEGIN
{sql}
END
";
        }

        public override string GetReadAllChunksSql(int limit)
        {
            var top = limit != Int32.MaxValue ? $"TOP {limit}" : "";

            return $@"SELECT {top} 
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM 
                        [{this.StreamsTableName}] 
                      WHERE 
                          [Position] >= @fromPositionInclusive 
                      ORDER BY 
                          [Position]";
            
        }

        public override string GetSelectLastPositionSql()
        {
            return $@"SELECT TOP 1
                        [Position]
                      FROM 
                        [{this.StreamsTableName}] 
                      ORDER BY 
                          [Position] DESC";
        }
    }
}