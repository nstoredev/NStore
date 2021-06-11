using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Sample.Support;

namespace NStore.Sample
{
    static class Program
    {
        private static string Mongo = "mongodb://localhost/NStoreSample";
        private static readonly CommandLineApplication Cmd = new CommandLineApplication(throwOnUnexpectedArg: false);

        private static string _providerName = "memory";
        private static bool _useSnapshots = true;
        private static bool _quietMode = false;
        private static bool _fastMode = false;

        static async Task Main(string[] args)
        {
            ParseCommandLine(args);

            var store = await BuildStore(_providerName);

            using var app = new SampleApp(store, _providerName, _useSnapshots, _quietMode, _fastMode);

//            await app.Simple();

            Console.WriteLine("Press ENTER to start sequential stream write");
            Console.ReadLine();
            await app.WriteSequentialStream(10);
                
            app.DumpMetrics();
            app.StartPolling();

            Console.WriteLine("Press ENTER to start and wait projections, then press ENTER again to show data & stats.");
            Console.ReadLine();
            await app.CreateRooms(32);

            app.DumpMetrics();

            await app.AddSomeBookings(1_024);
            app.DumpMetrics();

            await app.PollToEnd();
            Console.ReadLine();

            app.ShowRooms();
            app.DumpMetrics();

            Console.WriteLine("Press ENTER to close.");
            Console.ReadLine();
        }

        static void ParseCommandLine(string[] args)
        {
            var mongo = Cmd.Option("-m|--mongo", "Use mongo as storage", CommandOptionType.NoValue);
            var snapshots = Cmd.Option("-s|--snapshots", "Use snapsthos", CommandOptionType.NoValue);
            var quietmode = Cmd.Option("-q|--quiet", "Quiet mode", CommandOptionType.NoValue);
            var fastmode = Cmd.Option("-f|--fast", "Fast mode: latency @ 1ms", CommandOptionType.NoValue);

            Cmd.HelpOption("-? | -h | --help");

            Cmd.OnExecute(() =>
            {
                if (mongo.HasValue())
                {
                    _providerName = "mongo";
                }

                _useSnapshots = snapshots.HasValue();
                _quietMode = quietmode.HasValue();
                _fastMode = fastmode.HasValue();
                return 0;
            });

            Cmd.Execute(args);
        }

        static async Task<IPersistence> BuildStore(string store)
        {
            Console.WriteLine($"Selected store is {store}");

            switch (store.ToLowerInvariant())
            {
                case "memory":
                    {
                        var network = new ReliableNetworkSimulator(2, 10);
                        var options = new InMemoryPersistenceOptions
                        {
                            CloneFunc = ObjectSerializer.Clone,
                            NetworkSimulator = network
                        };
                        return new InMemoryPersistence(options);
                    }

                case "mongo":
                    {
                        var options = new MongoPersistenceOptions
                        {
                            PartitionsConnectionString = Mongo,
                            UseLocalSequence = true,
                            PartitionsCollectionName = "partitions",
                            SequenceCollectionName = "seq",
                            DropOnInit = true,
//                            MongoPayloadSerializer = new DiagnosticSerializerWrapper(new MongoCustomMongoPayloadSerializer()),
//                            MongoPayloadSerializer = new MongoCustomMongoPayloadSerializer(),
                            CustomizePartitionSettings = settings =>
                            {
                                settings.MaxConnectionPoolSize = 5000;
                            }
                        };
                        var mongo = new MongoPersistence(options);
                        Console.WriteLine("Init mongo storage");
                        await mongo.InitAsync(CancellationToken.None);
                        Console.WriteLine("Mongo storage ready");
                        return mongo;
                    }
            }

            throw new Exception($"Invalid store {store}");
        }
    }
}