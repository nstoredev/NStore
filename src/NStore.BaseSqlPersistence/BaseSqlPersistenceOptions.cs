using NStore.Core.Logging;
using NStore.Core.Persistence;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.BaseSqlPersistence
{
    public abstract class BaseSqlPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }
        public string ConnectionString { get; set; }
        public string StreamsTableName { get; set; }

        /// <summary>
        /// When stream idempotency is enabled OperationsId must be unique:
        /// with two or more chunks with same OperationId only the first one
        /// is stored, others are skipped without errors. 
        /// </summary>
        public bool StreamIdempotencyEnabled { get; set; } = true;
        public ISqlPayloadSerializer Serializer { get; set; }

        protected BaseSqlPersistenceOptions(INStoreLoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            StreamsTableName = "Streams";
        }

        public abstract string GetSelectLastPositionSql();
        public abstract string GetSelectLastChunkSql();
        public virtual string GetDeleteStreamChunksSql()
        {
            return $@"DELETE FROM [{StreamsTableName}] WHERE 
                          [PartitionId] = @PartitionId 
                      AND [Index] BETWEEN @fromLowerIndexInclusive AND @toUpperIndexInclusive";
        }
        public virtual string GetSelectChunkByStreamAndOperation()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM [{StreamsTableName}] 
                      WHERE [PartitionId] = @PartitionId AND [OperationId] = @OperationId";
        }
        public virtual string GetSelectAllChunksByOperationSql()
        {
            return $@"SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM [{StreamsTableName}] 
                      WHERE [OperationId] = @OperationId";
        }
        public abstract string GetInsertChunkSql();
        public virtual string GetReplaceChunkSql()
        {
            return $@"UPDATE [{StreamsTableName}]
                    SET [PartitionId] = @PartitionId,
                        [Index] = @Index,
                        [Payload] = @Payload,
                        [OperationId] = @OperationId,
                        [SerializerInfo] = @SerializerInfo
                    WHERE [Position] = @Position";
        }
        public abstract string GetCreateTableIfMissingSql();
        public abstract string GetReadAllChunksSql(int limit);
        public virtual string GetChunkByPositionSql()
        {
            return $@"SELECT  
                        [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
                      FROM 
                        [{StreamsTableName}] 
                      WHERE 
                          [Position] = @Position";
        }

        public abstract string GetRangeSelectChunksSql(
            long lowerIndexInclusive,
            long upperIndexInclusive,
            int limit,
            bool descending
        );

        public abstract Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Generating a multi-partition range select SQL query, that is common between the various implementations
        /// because it is created using standard SQL syntax.
        /// 
        /// Introduced with batch operation we start avoiding duplication of code, if we have a standard SQL syntax
        /// we can use as a base implementation for all SQL based persistences, if needed we can override it in specific
        /// implementations.
        /// </summary>
        /// <param name="partitionIdsList"></param>
        /// <param name="lowerIndexInclusive"></param>
        /// <param name="upperIndexInclusive"></param>
        /// <param name="descending"></param>
        /// <returns></returns>
        public virtual string GetRangeMultiplePartitionSelectChunksSql(
            IEnumerable<string> partitionIdsList,
            long lowerIndexInclusive,
            long upperIndexInclusive,
            bool descending)
        {
            var sb = new StringBuilder("SELECT ");

            sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
            sb.Append($"FROM {StreamsTableName} ");
            //Generate a query like [PartitionId] in (@p1, @p2, @p3) based on how many parameter we have
            sb.Append($"WHERE [PartitionId] in ({String.Join(",", Enumerable.Range(1, partitionIdsList.Count()).Select(n => $"@p{n}"))}) ");

            if (lowerIndexInclusive > 0)
            {
                sb.Append("AND [Index] >= @lowerIndexInclusive ");
            }

            if (upperIndexInclusive > 0 && upperIndexInclusive != Int64.MaxValue)
            {
                sb.Append("AND [Index] <= @upperIndexInclusive ");
            }

            sb.Append(descending ? "ORDER BY [Index] DESC" : "ORDER BY [Index]");

            return sb.ToString();
        }

        /// <summary>
        /// Generates a SQL query to read multiple partitions where each partition can have its own index range.
        /// Uses UNION ALL to combine results from different partitions while maintaining per-partition ordering.
        /// </summary>
        /// <param name="partitionRequests">Collection of partition read requests with individual ranges</param>
        /// <returns>SQL query string with UNION ALL clauses</returns>
        /// <remarks>
        /// Performance Note: For large numbers of partitions (>20), consider using a CTE or temp table approach.
        /// The current UNION ALL approach is optimal for small to medium partition counts.
        /// </remarks>
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

        /// <summary>
        /// Generates a SQL query to read multiple partitions backward where each partition can have its own index range.
        /// Uses UNION ALL to combine results from different partitions while maintaining per-partition descending ordering.
        /// </summary>
        /// <param name="partitionRequests">Collection of partition read requests with individual ranges</param>
        /// <returns>SQL query string with UNION ALL clauses and descending order</returns>
        public virtual string GetRangeMultiplePartitionWithRangesSelectChunksSqlBackward(
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

            // Order by Index descending for backward reading
            sb.Append(" ORDER BY [Index] DESC");

            return sb.ToString();
        }

        /// <summary>
        /// Generates a SQL query to get the last chunk (highest index) for each partition.
        /// Uses ROW_NUMBER() window function to efficiently select the top record per partition.
        /// </summary>
        /// <param name="partitionIds">Collection of partition IDs to query</param>
        /// <returns>SQL query string that returns the last chunk for each partition</returns>
        public virtual string GetLastChunkForPartitionsSql(IEnumerable<string> partitionIds)
        {
            var partitionList = partitionIds.ToList();
            if (!partitionList.Any())
            {
                return string.Empty;
            }

            var sb = new StringBuilder();

            // Use ROW_NUMBER() to rank chunks within each partition by index descending
            // Then filter to only get the top-ranked (last) chunk per partition
            sb.Append("WITH RankedChunks AS (");
            sb.Append("SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo], ");
            sb.Append("ROW_NUMBER() OVER (PARTITION BY [PartitionId] ORDER BY [Index] DESC) AS rn ");
            sb.Append($"FROM {StreamsTableName} ");
            sb.Append($"WHERE [PartitionId] IN ({string.Join(",", Enumerable.Range(0, partitionList.Count).Select(n => $"@p{n}"))})");
            sb.Append(") ");
            sb.Append("SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
            sb.Append("FROM RankedChunks WHERE rn = 1");

            return sb.ToString();
        }
    }
}