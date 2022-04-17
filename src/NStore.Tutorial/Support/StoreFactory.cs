using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Persistence.MsSql;

namespace NStore.Tutorial.Support
{
    public static class StoreFactory
    {
        public static Task<IStore> CreateInMemoryAsync()
        {
            // Cloning function allow safe operations avoiding shared 
            // data between snapshots, aggregates, streams.
            //
            // Mimic (de)serialization of other persistence providers
            return Task.FromResult<IStore>(new InMemoryStore(SerializationHelper.DeepClone));
        }

        public static async Task<IStore> CreateSqlServerAsync(
            string connectionString,
            string tablename,
            INStoreLoggerFactory loggerFactory)
        {
            var options = new MsSqlStoreOptions(loggerFactory)
            {
                ConnectionString = connectionString,
                StreamsTableName = tablename,
                Serializer = new JsonMsSqlSerializer()
            };
            var persistence = new MsSqlStore(options);

            await persistence.DestroyAllAsync(CancellationToken.None);
            await persistence.InitAsync(CancellationToken.None);

            return persistence;
        }
    }
}