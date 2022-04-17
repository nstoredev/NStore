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
        public IStore StreamsStore { get; }

        public IStore SnapshotsStore { get; }
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

        private TutorialRuntime(IStore streamsStore, IStore snapshotsStore)
        {
            StreamsStore = streamsStore;
            SnapshotsStore = snapshotsStore;
            Configure();

            Logger.LogInformation("Runtime Started");

            // configure aggregate factory delegation to DI container
            _aggregateFactory = new AggregateFactory(
                aggregateType => (IAggregate) this._serviceProvider.GetService(aggregateType)
            );

            _streamsFactory = new StreamsFactory(Instrument(streamsStore, "streams"));
            _snapshotStore = new DefaultSnapshotStore(Instrument(snapshotsStore, "snapshots"));
        }

        public IStore Instrument(IStore store, string name)
        {
            return new LogDecorator(store, _loggerFactory, name);
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
            var persistence = await StoreFactory.CreateInMemoryAsync();
            var snapshots = await StoreFactory.CreateInMemoryAsync();

            var runtime = new TutorialRuntime(persistence, snapshots);

            return runtime;
        }

        public static async Task<TutorialRuntime> UseSqlServer(string connectionString)
        {
            var persistence = await StoreFactory.CreateSqlServerAsync
            (
                connectionString,
                "streams",
                NStoreNullLoggerFactory.Instance
            );
            
            var snapshots = await StoreFactory.CreateSqlServerAsync
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