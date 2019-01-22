using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using NStore.Core.Streams;
using NStore.Domain;
using NStore.Tutorial.CartDomain;
using NStore.Tutorial.Support;

namespace NStore.Tutorial
{
    public class ApplicationRuntime
    {
        private readonly IAggregateFactory _aggregateFactory;
        private readonly IStreamsFactory _streamsFactory;
        private readonly ISnapshotStore _snapshotStore;
        private ServiceProvider _serviceProvider;

        private readonly ILogger<ApplicationRuntime> _logger;
        private readonly INStoreLoggerFactory _loggerFactory;

        private ApplicationRuntime(
            IPersistence persistence,
            IPersistence snapshots)
        {
            Startup();

            _loggerFactory = new ConsoleLoggerFactory(_serviceProvider.GetRequiredService<ILoggerFactory>());

            _logger = _serviceProvider.GetService<ILogger<ApplicationRuntime>>();
            _logger.LogInformation("Runtime Started");

            _aggregateFactory = new AggregateFactory(
                aggregateType => (IAggregate)this._serviceProvider.GetService(aggregateType)
            );

            _streamsFactory = new StreamsFactory(Instrument(persistence, "streams"));
            _snapshotStore = new DefaultSnapshotStore(Instrument(snapshots, "snapshots"));
        }

        private IPersistence Instrument(IPersistence persistence, string name)
        {
            return new LogDecorator(persistence, _loggerFactory, name);
        }

        public void Shutdown()
        {
            _logger.LogInformation("Shutting down... bye");
            _serviceProvider.Dispose();
        }

        public IRepository CreateRepository()
        {
            return new Repository(
                _aggregateFactory,
                _streamsFactory,
                _snapshotStore
            );
        }

        public static ApplicationRuntime CreateDefaultRuntime()
        {
            var persistence = PersistenceFactory.CreateInMemory();
            var snapshots = PersistenceFactory.CreateInMemory();

            var runtime = new ApplicationRuntime(persistence,snapshots);

            return runtime;
        }


        private void Startup()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(configure =>
                configure.AddConsole()
                    .SetMinimumLevel(LogLevel.Trace)
                );

            // add domain
            serviceCollection.AddTransient<ShoppingCart>();

            _serviceProvider = serviceCollection.BuildServiceProvider();
        }
    }
}