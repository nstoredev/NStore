using NStore.BaseSqlPersistence;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistenceOptions : BaseSqlPersistenceOptions
    {
        /// <summary>
        /// Maximum number of retry attempts for transient SQL errors.
        /// Default is 3. Set to 0 to disable retries.
        /// </summary>
        public int MaxRetryAttempts { get; set; } = 3;

        /// <summary>
        /// Delay in milliseconds between retry attempts.
        /// Uses exponential backoff: delay * (2 ^ attemptNumber).
        /// Default is 100ms.
        /// </summary>
        public int RetryDelayMilliseconds { get; set; } = 100;

        /// <summary>
        /// Maximum connection pool size. If not set, uses SQL Server default (100).
        /// Adjust based on your application's concurrency requirements.
        /// </summary>
        public int? MaxPoolSize { get; set; }

        /// <summary>
        /// Minimum connection pool size. If not set, uses SQL Server default (0).
        /// </summary>
        public int? MinPoolSize { get; set; }

        /// <summary>
        /// Command timeout in seconds. Default is 30 seconds.
        /// Set to 0 for infinite timeout (not recommended for production).
        /// </summary>
        public int CommandTimeoutSeconds { get; set; } = 30;

        public MsSqlPersistenceOptions(INStoreLoggerFactory loggerFactory) : base(loggerFactory)
        {
        }

        protected virtual string GetCreateTableSql()
        {
            string idempotencySql = StreamIdempotencyEnabled
                ? $"CREATE UNIQUE INDEX IX_{StreamsTableName}_OPID on dbo.{StreamsTableName} (PartitionId, OperationId)"
                : string.Empty;

            string idempotencyConstraint = StreamIdempotencyEnabled
                ? "NOT NULL"
                : "NULL";

            return $@"CREATE TABLE [{StreamsTableName}](
                [Position] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                [PartitionId] NVARCHAR(255) NOT NULL,
                [OperationId] NVARCHAR(255) {idempotencyConstraint},
                [SerializerInfo] NVARCHAR(255) NOT NULL,
                [Index] BIGINT NOT NULL,
                [Payload] VARBINARY(MAX)
            )

            {idempotencySql}
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

        public override string GetDeleteStreamChunksSql()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
        }

        public override string GetSelectChunkByStreamAndOperation()
        {
            if (!StreamIdempotencyEnabled)
            {
                throw new MsSqlPersistenceException("Stream idempotency is disabled. Cannot search by OperationId");
            }

            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM [{StreamsTableName}] 
                      WHERE [PartitionId] = @PartitionId AND [OperationId] = @OperationId";
        }

        public override string GetSelectAllChunksByOperationSql()
        {
            if (!StreamIdempotencyEnabled)
            {
                throw new MsSqlPersistenceException("Stream idempotency is disabled. Cannot search by OperationId");
            }

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
            long lowerIndexInclusive,
            long upperIndexInclusive,
            int limit,
            bool descending
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

        public override async Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(ConnectionString);
            
            // Apply connection pool settings if specified
            if (MaxPoolSize.HasValue)
            {
                connectionStringBuilder.MaxPoolSize = MaxPoolSize.Value;
            }
            
            if (MinPoolSize.HasValue)
            {
                connectionStringBuilder.MinPoolSize = MinPoolSize.Value;
            }

            // Set command timeout (connection string uses "Connection Timeout" for connection, not command)
            connectionStringBuilder.ConnectTimeout = Math.Max(15, CommandTimeoutSeconds);

            var connection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }

            return new MsSqlContext(connection);
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
if not exists (select * from dbo.sysobjects where id = object_id(N'{StreamsTableName}') and OBJECTPROPERTY(id, N'IsUserTable') = 1) 
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

        public override string GetChunkByPositionSql()
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM 
                        [{this.StreamsTableName}] 
                      WHERE 
                          [Position] = @Position";
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

        public override string GetRangeMultiplePartitionSelectChunksSql(
            IEnumerable<string> partitionIdsList,
            long lowerIndexInclusive,
            long upperIndexInclusive,
            bool descending)
        {
            var sb = new StringBuilder("SELECT ");

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
            sb.Append($"FROM {StreamsTableName} ");
            //Generate a query like [PartitionId] in (@p1, @p2, @p3) based on how many parameter we have in the list
            sb.Append($"WHERE [PartitionId] in ({String.Join(",", Enumerable.Range(1, partitionIdsList.Count()).Select(n => $"@p{n}"))}) ");

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

        /// <summary>
        /// Generates SQL query for reading multiple partitions where each partition has its own index range.
        /// Uses UNION ALL to combine results from different partition ranges.
        /// </summary>
        public virtual string GetRangeMultiplePartitionWithRangesSelectChunksSql(
            IEnumerable<PartitionReadRequest> partitionRequests)
        {
            var requests = partitionRequests.ToList();
            if (!requests.Any())
            {
                return string.Empty;
            }

            var sb = new StringBuilder();
            
            for (int i = 0; i < requests.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(" UNION ALL ");
                }

                sb.Append("SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
                sb.Append($"FROM {StreamsTableName} ");
                sb.Append($"WHERE [PartitionId] = @p{i} ");

                var request = requests[i];
                
                if (request.FromPartitionIndexInclusive > 0)
                {
                    sb.Append($"AND [Index] >= @from{i} ");
                }

                if (request.ToPartitionIndexInclusive != long.MaxValue)
                {
                    sb.Append($"AND [Index] <= @to{i} ");
                }
            }

            // Order by Index to maintain ordering guarantees within each partition
            sb.Append(" ORDER BY [Index]");

            return sb.ToString();
        }
    }
}