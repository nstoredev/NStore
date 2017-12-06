using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Threading;
using System.Threading.Tasks;
using NStore.BaseSqlPersistence;
using NStore.Core.Persistence;

namespace NStore.Persistence.MsSql
{
    public class MsSqlPersistence : AbstractSqlPersistence, IPersistence
    {
        private const int DuplicatedIndexExceptionErrorNumber = 2601;
        private int REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE = 0;

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

            using (var connection = Connect())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var command = CreateCommand(sql, connection))
                {
                    AddParam(command, "@fromPositionInclusive", fromPositionInclusive);

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
            using (var conn = Connect())
            {
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                var sql = _options.GetDropTableSql();
                using (var cmd = CreateCommand(sql, conn))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        protected override DbConnection Connect()
        {
            return new SqlConnection(_options.ConnectionString);
        }

        protected override DbCommand CreateCommand(string sql, DbConnection connection)
        {
            return new SqlCommand(sql, (SqlConnection)connection);
        }

        protected override DbCommand CreateCommand(string sql, DbConnection connection, DbTransaction transaction)
        {
            return new SqlCommand(sql, (SqlConnection)connection, (SqlTransaction)transaction);
        }

        protected override void AddParam(DbCommand command, string paramName, object value)
        {
            if (value is byte[] bytes)
            {
                ((SqlCommand)command).Parameters.AddWithValue(paramName, new SqlBinary(bytes));
            }
            else
            {
                ((SqlCommand)command).Parameters.AddWithValue(paramName, value);
            }
        }

        protected override long GenerateIndex()
        {
            return Interlocked.Increment(ref REFACTOR_TO_USE_SEQUENCE_OR_NOT_STRICTLY_SEQUENTIAL_VALUE);
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