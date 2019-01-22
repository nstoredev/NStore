using System;
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
        private ILogger<TutorialRuntime> _logger;
        private INStoreLoggerFactory _loggerFactory;

        private TutorialRuntime(IPersistence persistence, IPersistence snapshots)
        {
            Configure();

            _logger.LogInformation("Runtime Started");

            // configure aggregate factory delegation to DI container
            _aggregateFactory = new AggregateFactory(
                aggregateType => (IAggregate) this._serviceProvider.GetService(aggregateType)
            );

            _streamsFactory = new StreamsFactory(Instrument(persistence, "streams"));
            _snapshotStore = new DefaultSnapshotStore(Instrument(snapshots, "snapshots"));
        }

        public void Log(string message)
        {
            _logger.LogInformation(message);
        }

        private IPersistence Instrument(IPersistence persistence, string name)
        {
            return new LogDecorator(persistence, _loggerFactory, name);
        }

        public void Shutdown()
        {
            _logger.LogInformation("Shutting down... bye");
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
                .WriteTo.Console()
                .MinimumLevel.Verbose()
                .CreateLogger();
            
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(configure =>
                configure.AddSerilog()
            );

            // add domain
            serviceCollection.AddTransient<ShoppingCart>();

            _serviceProvider = serviceCollection.BuildServiceProvider();

            _loggerFactory = new ConsoleLoggerFactory(_serviceProvider.GetRequiredService<ILoggerFactory>());
            _logger = _serviceProvider.GetService<ILogger<TutorialRuntime>>();
        }

        /// <summary>
        /// Default runtime factory
        /// </summary>
        /// <returns>Runtime</returns>
        public static TutorialRuntime CreateDefaultRuntime()
        {
            var persistence = PersistenceFactory.CreateInMemory();
            var snapshots = PersistenceFactory.CreateInMemory();

            var runtime = new TutorialRuntime(persistence, snapshots);

            return runtime;
        }

        public IStream OpenStream(string id)
        {
            return _streamsFactory.Open(id);
        }
    }
}