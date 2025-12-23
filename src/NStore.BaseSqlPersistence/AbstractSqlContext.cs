using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NStore.BaseSqlPersistence
{
    public abstract class AbstractSqlContext : IDisposable, IAsyncDisposable
    {
        public DbConnection Connection { get; }
        private readonly bool _disposeConnection;

        protected AbstractSqlContext(DbConnection connection, bool disposeConnection)
        {
            Connection = connection;
            _disposeConnection = disposeConnection;
        }

        public void Dispose()
        {
            if (_disposeConnection)
            {
                Connection.Dispose();
            }
        }

#if NET8_0_OR_GREATER
        public async ValueTask DisposeAsync()
        {
            if (_disposeConnection)
            {
                await Connection.DisposeAsync().ConfigureAwait(false);
            }
            GC.SuppressFinalize(this);
        }
#else
        public ValueTask DisposeAsync()
        {
            if (_disposeConnection)
            {
                Connection.Dispose();
            }
            GC.SuppressFinalize(this);
            return default;
        }
#endif

        public abstract DbCommand CreateCommand(string sql);

        public abstract DbCommand CreateCommand(string sql, DbTransaction transaction);
        public abstract void AddParam(DbCommand command, string paramName, object value);
    }
}