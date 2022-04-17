using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.BaseSqlPersistence
{
    public abstract class BaseSqlStoreOptions
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

        protected BaseSqlStoreOptions(INStoreLoggerFactory loggerFactory)
        {
            LoggerFactory = loggerFactory;
            StreamsTableName = "Streams";
        }

        public abstract string GetSelectLastPositionSql();
        public abstract string GetSelectLastChunkSql();
        public abstract string GetDeleteStreamChunksSql();
        public abstract string GetSelectChunkByStreamAndOperation();
        public abstract string GetSelectAllChunksByOperationSql();
        public abstract string GetInsertChunkSql();
        public abstract string GetReplaceChunkSql();
        public abstract string GetCreateTableIfMissingSql();
        public abstract string GetReadAllChunksSql(int limit);
        public abstract string GetChunkByPositionSql();
        
        public abstract string GetRangeSelectChunksSql(
            long upperIndexInclusive,
            long lowerIndexInclusive,
            int limit,
            bool descending
        );

        public abstract Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken);
    }
}