using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Persistence.MsSql;

namespace NStore.Tutorial.Support
{
    public static class PersistenceFactory
    {
        public static Task<IPersistence> CreateInMemoryAsync()
        {
            // Cloning function allow safe operations avoiding shared 
            // data between snapshots, aggregates, streams.
            //
            // Mimic (de)serialization of other persistence providers
            return Task.FromResult<IPersistence>(new InMemoryStore(SerializationHelper.DeepClone));
        }

        public static async Task<IPersistence> CreateSqlServerAsync(
            string connectionString,
            string tablename,
            INStoreLoggerFactory loggerFactory)
        {
            var options = new MsSqlPersistenceOptions(loggerFactory)
            {
                ConnectionString = connectionString,
                StreamsTableName = tablename,
                Serializer = new JsonMsSqlSerializer()
            };
            var persistence = new MsSqlPersistence(options);

            await persistence.DestroyAllAsync(CancellationToken.None);
            await persistence.InitAsync(CancellationToken.None);

            return persistence;
        }
    }
}