using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;
using NStore.Domain;
using NStore.Tutorial.CartDomain;
using NStore.Tutorial.Support;
using Serilog;

namespace NStore.Tutorial
{
    /// <summary>
    /// Tutorial runtime.
    /// </summary>
    public class TutorialRuntime
    {
        public IPersistence StreamsPersistence { get; }

        public IPersistence SnapshotsPersistence { get; }
        //
        // Core stuff
        //

        // Build aggregate instances
        private readonly IAggregateFactory _aggregateFactory;

        // streams management
        private readonly IStreamsFactory _streamsFactory;

        // snapshots persistence
        private readonly ISnapshotStore _snapshotStore;

        //
        // Support stuff
        //

        // for DI Enabled Aggregates 
        private ServiceProvider _serviceProvider;

        // logging
        private INStoreLoggerFactory _loggerFactory;

        public ILogger<TutorialRuntime> Logger { get; private set; }

        private TutorialRuntime(IPersistence streamsPersistence, IPersistence snapshotsPersistence)
        {
            StreamsPersistence = streamsPersistence;
            SnapshotsPersistence = snapshotsPersistence;
            Configure();

            Logger.LogInformation("Runtime Started");

            // configure aggregate factory delegation to DI container
            _aggregateFactory = new AggregateFactory(
                aggregateType => (IAggregate) this._serviceProvider.GetService(aggregateType)
            );

            _streamsFactory = new StreamsFactory(Instrument(streamsPersistence, "streams"));
            _snapshotStore = new DefaultSnapshotStore(Instrument(snapshotsPersistence, "snapshots"));
        }

        public IPersistence Instrument(IPersistence persistence, string name)
        {
            return new LogDecorator(persistence, _loggerFactory, name);
        }

        public void Shutdown()
        {
            _serviceProvider.Dispose();
            
            Serilog.Log.CloseAndFlush();
        }

        public IRepository CreateRepository()
        {
            return new Repository(
                _aggregateFactory,
                _streamsFactory,
                _snapshotStore
            );
        }

        private void Configure()
        {
            Serilog.Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .Enrich.WithCaller()
                .WriteTo.Console(outputTemplate:
                    "[{Timestamp:HH:mm:ss} {SourceContext:l} {Level:u3}]{NewLine}{Message:lj}{NewLine}at {Caller}{NewLine}{Exception}{NewLine}")
                .MinimumLevel.Verbose()
                .CreateLogger();
            
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(configure =>
                configure.AddSerilog()
            );

            // add domain
            serviceCollection.AddTransient<ShoppingCart>();

            _serviceProvider = serviceCollection.BuildServiceProvider();

            _loggerFactory = new LoggerFactoryAdapter(
                _serviceProvider.GetRequiredService<ILoggerFactory>()
            );
            Logger = _serviceProvider.GetService<ILogger<TutorialRuntime>>();
        }

        public static Func<Task<TutorialRuntime>> Initializer = () => null;
        
        /// <summary>
        /// Default runtime factory
        /// </summary>
        /// <returns>Runtime</returns>
        public static async Task<TutorialRuntime> UseInMemory()
        {
            var persistence = await PersistenceFactory.CreateInMemoryAsync();
            var snapshots = await PersistenceFactory.CreateInMemoryAsync();

            var runtime = new TutorialRuntime(persistence, snapshots);

            return runtime;
        }

        public static async Task<TutorialRuntime> UseSqlServer(string connectionString)
        {
            var persistence = await PersistenceFactory.CreateSqlServerAsync
            (
                connectionString,
                "streams",
                NStoreNullLoggerFactory.Instance
            );
            
            var snapshots = await PersistenceFactory.CreateSqlServerAsync
            (
                connectionString,
                "snapshots",
                NStoreNullLoggerFactory.Instance
            );
            var runtime = new TutorialRuntime(persistence, snapshots);

            return runtime;
        }

        public IStream OpenStream(string id)
        {
            return _streamsFactory.Open(id);
        }
    }
}