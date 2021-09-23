using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using NStore.BaseSqlPersistence;
using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistence : AbstractSqlPersistence, IPersistence
    {
        private const int DuplicatedIndexExceptionErrorNumber = 2601;

        private readonly MsSqlPersistenceOptions _options;

        public bool SupportsFillers => false;

        public MsSqlPersistence(MsSqlPersistenceOptions options) : base(options)
        {
            _options = options;

            if (_options.Serializer == null)
            {
                throw new MsSqlPersistenceException("MsSqlOptions should provide a custom Serializer");
            }
        }

        public async Task ReadAllAsync(
            long fromPositionInclusive,
            ISubscription subscription,
            int limit,
            CancellationToken cancellationToken)
        {
            var sql = _options.GetReadAllChunksSql(limit);

            using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                using (var command = context.CreateCommand(sql))
                {
                    context.AddParam(command, "@fromPositionInclusive", fromPositionInclusive);

                    await PushToSubscriber(command, fromPositionInclusive, subscription, true, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            await EnsureTable(cancellationToken).ConfigureAwait(false);
        }

        public async Task DestroyAllAsync(CancellationToken cancellationToken)
        {
            using (var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false))
            {
                var sql = _options.GetDropTableSql();
                using (var cmd = context.CreateCommand(sql))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected override bool IsDuplicatedStreamOperation(Exception exception)
        {
            return exception is SqlException ex &&
                   (ex.Number == DuplicatedIndexExceptionErrorNumber && ex.Message.Contains("_OPID"));
        }

        protected override bool IsDuplicatedStreamIndex(Exception exception)
        {
            return exception is SqlException ex &&
                   (ex.Number == DuplicatedIndexExceptionErrorNumber && ex.Message.Contains("_IDX"));
        }
    }
}