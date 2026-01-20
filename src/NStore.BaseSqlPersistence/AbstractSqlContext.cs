using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NStore.BaseSqlPersistence
{
    public abstract class AbstractSqlContext : IDisposable, IAsyncDisposable
    {
        public DbConnection Connection { get; }
        private readonly bool _disposeConnection;
        private bool _disposed;

        protected AbstractSqlContext(DbConnection connection, bool disposeConnection)
        {
            Connection = connection;
            _disposeConnection = disposeConnection;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing && _disposeConnection)
                {
                    Connection.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            if (_disposeConnection)
            {
#if NET8_0_OR_GREATER
                await Connection.DisposeAsync().ConfigureAwait(false);
#else
                Connection.Dispose();
#endif
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                await DisposeAsyncCore().ConfigureAwait(false);
                _disposed = true;
            }
            
            Dispose(disposing: false);
            GC.SuppressFinalize(this);
        }

        public abstract DbCommand CreateCommand(string sql);

        public abstract DbCommand CreateCommand(string sql, DbTransaction transaction);
        public abstract void AddParam(DbCommand command, string paramName, object value);
    }
}