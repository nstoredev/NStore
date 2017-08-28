using System;
using System.Threading;
using Microsoft.Extensions.CommandLineUtils;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Persistence.Mongo;
using NStore.Persistence;
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

        static void Main(string[] args)
        {
            ParseCommandLine(args);
            
            var store = BuildStore(_providerName);

            using (var app = new SampleApp(store, _providerName, _useSnapshots,_quietMode, _fastMode))
            {
                Console.WriteLine(
                    "Press ENTER to start sequential stream write");
                Console.ReadLine();
                app.WriteSequentialStream(1000)
                    .GetAwaiter().GetResult();
                app.DumpMetrics();

                app.StartPolling();

                Console.WriteLine(
                    "Press ENTER to start and wait projections, then press ENTER again to show data & stats.");
                Console.ReadLine();
                app.CreateRooms(32)
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                
                app.DumpMetrics();

                app.AddSomeBookings(1_024)
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                app.DumpMetrics();

                app.PollToEnd()
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                Console.ReadLine();

                app.ShowRooms();
                app.DumpMetrics();

                Console.WriteLine("Press ENTER to close.");
                Console.ReadLine();
            }
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

        static IPersistence BuildStore(string store)
        {
            Console.WriteLine($"Selected store is {store}");

            switch (store.ToLowerInvariant())
            {
                case "memory":
                {
                    var network = new ReliableNetworkSimulator(2, 10);
                    return new InMemoryPersistence(network, ObjectSerializer.Clone);
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
                        Serializer = new MongoCustomSerializer(),
                        CustomizePartitionSettings = settings =>
                        {
                            settings.MaxConnectionPoolSize = 5000;
                        }
                    };
                    var mongo = new MongoPersistence(options);
                    mongo.InitAsync(CancellationToken.None).GetAwaiter().GetResult();
                    return mongo;
                }
            }

            throw new Exception($"Invalid store {store}");
        }
    }
}