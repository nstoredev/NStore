using System.Data.Common;
using Microsoft.Data.Sqlite;
using NStore.BaseSqlPersistence;

namespace NStore.Persistence.Sqlite
{
    public class SqliteContext : AbstractSqlContext
    {
        public SqliteContext(DbConnection connection, bool disposeConnection) : base(connection, disposeConnection)
        {
        }

        public override DbCommand CreateCommand(string sql)
        {
            return new SqliteCommand(sql, (SqliteConnection)Connection);
        }

        public override DbCommand CreateCommand(string sql, DbTransaction transaction)
        {
            return new SqliteCommand(sql, (SqliteConnection)Connection, (SqliteTransaction)transaction);
        }
        
        public override void AddParam(DbCommand command, string paramName, object value)
        {
            if (value is byte[] bytes)
            {
                ((SqliteCommand)command).Parameters.Add(paramName, SqliteType.Blob).Value = bytes;
            }
            else
            {
                ((SqliteCommand)command).Parameters.AddWithValue(paramName, value);
            }
        }
    }
}