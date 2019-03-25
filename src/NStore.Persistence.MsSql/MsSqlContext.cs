using NStore.BaseSqlPersistence;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace NStore.Persistence.MsSql
{
    public class MsSqlContext : AbstractSqlContext
    {
        private readonly SqlTransaction _pendingTransaction;

        public MsSqlContext(DbConnection connection) : base(connection, true)
        {
        }

        public MsSqlContext(DbConnection connection, SqlTransaction transaction) : base(connection, false)
        {
            _pendingTransaction = transaction;
        }

        public override DbCommand CreateCommand(string sql)
        {
            return new SqlCommand(sql, (SqlConnection)Connection, _pendingTransaction);
        }

        public override DbCommand CreateCommand(string sql, DbTransaction transaction)
        {
            return new SqlCommand(sql, (SqlConnection)Connection, (SqlTransaction)transaction);
        }

        public override void AddParam(DbCommand command, string paramName, object value)
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
    }
}