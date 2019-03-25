using System;
using System.Data.Common;

namespace NStore.BaseSqlPersistence
{
    public abstract class AbstractSqlContext : IDisposable
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

        public abstract DbCommand CreateCommand(string sql);

        public abstract DbCommand CreateCommand(string sql, DbTransaction transaction);
        public abstract void AddParam(DbCommand command, string paramName, object value);
    }
}