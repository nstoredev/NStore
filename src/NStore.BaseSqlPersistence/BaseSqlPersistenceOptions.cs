using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.BaseSqlPersistence
{
    public abstract class BaseSqlPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; set; }
        public string ConnectionString { get; set; }
        public string StreamsTableName { get; set; }
        public ISqlPayloadSerializer Serializer { get; set; }

        protected BaseSqlPersistenceOptions(INStoreLoggerFactory loggerFactory)
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
        public abstract string GetCreateTableIfMissingSql();
        public abstract string GetReadAllChunksSql(int limit);

        public abstract string GetRangeSelectChunksSql(
            long upperIndexInclusive,
            long lowerIndexInclusive,
            int limit,
            bool descending
        );

        public abstract Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken);
    }
}