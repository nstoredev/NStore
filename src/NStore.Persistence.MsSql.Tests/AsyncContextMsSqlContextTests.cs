using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Persistence.MsSql.Tests
{
    public class AsyncContextMsSqlContextTests : BasePersistenceTest
    {
        private AsyncContextMsSqlPersistenceOptions _contextConnectionOptions;
        private IMsSqlAsyncContext _mssqlcontext;
        protected override MsSqlPersistenceOptions CreateOptions()
        {
            var options = base.CreateOptions();

            _contextConnectionOptions = new AsyncContextMsSqlPersistenceOptions(options.LoggerFactory)
            {
                ConnectionString = options.ConnectionString,
                Serializer = options.Serializer,
                StreamsTableName = options.StreamsTableName
            };

            _mssqlcontext = _contextConnectionOptions;
            return _contextConnectionOptions;
        }

        [Fact]
        public async Task should_use_own_connection()
        {
            await this._store.AppendAsync("test", 0, "payload", "op1", CancellationToken.None)
                .ConfigureAwait(false);

            var recorder = await this._store.RecordAsync("test").ConfigureAwait(false);
            Assert.Equal(1, recorder.Length);
        }

        [Fact]
        public async Task should_use_external_connection()
        {
            // write with automatic connection
            await this._store.AppendAsync("test", 0, "payload", "op1", CancellationToken.None)
                .ConfigureAwait(false);

            // write with custom connection & transaction
            using (var connection = new SqlConnection(_contextConnectionOptions.ConnectionString))
            {
                await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);

                using (var transaction = connection.BeginTransaction())
                {
                    _mssqlcontext.Join(connection, transaction);

                    await this._store.AppendAsync("test", 1, "payload", "op2", CancellationToken.None)
                        .ConfigureAwait(false);

                    var contextrecorder = await this._store.RecordAsync("test").ConfigureAwait(false);
                    Assert.Equal(2, contextrecorder.Length);

                    transaction.Rollback();
                }
            }

            // read back
            _contextConnectionOptions.ClearAsyncContext();
            var recorder = await this._store.RecordAsync("test").ConfigureAwait(false);
            Assert.Equal(1, recorder.Length);
        }
    }
}
