using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using NStore.BaseSqlPersistence;
using NStore.Core.Logging;

namespace NStore.Persistence.MsSql
{
    public interface IMsSqlAsyncContext
    {
        void Join(DbConnection conn, SqlTransaction transaction);
    }

    public class AsyncContextMsSqlStoreOptions : MsSqlStoreOptions, IMsSqlAsyncContext
    {
        private readonly AsyncLocal<DbConnection> _connection = new AsyncLocal<DbConnection>();
        private readonly AsyncLocal<SqlTransaction> _transaction = new AsyncLocal<SqlTransaction>();

        public AsyncContextMsSqlStoreOptions(INStoreLoggerFactory loggerFactory) : base((INStoreLoggerFactory) loggerFactory)
        {
        }

        public void ClearAsyncContext()
        {
            _connection.Value = null;
            _transaction.Value = null;
        }

        public void Join(DbConnection conn, SqlTransaction transaction)
        {
            _connection.Value = conn;
            _transaction.Value = transaction;
        }

        public override Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken)
        {
            if (_connection.Value != null)
            {
                AbstractSqlContext context = new MsSqlContext(_connection.Value, _transaction.Value);
                return Task.FromResult(context);
            }

            return base.GetContextAsync(cancellationToken);
        }

    }
}