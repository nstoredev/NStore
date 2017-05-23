using System;
using Microsoft.Extensions.CommandLineUtils;
using MongoDB.Bson.Serialization;
using NStore.InMemory;
using NStore.Persistence.Mongo;
using NStore.Raw;
using NStore.Sample.Support;

namespace NStore.Sample
{
    class Program
    {
        private static string Mongo = "mongodb://localhost/NStoreSample";
        private static CommandLineApplication cmd = new CommandLineApplication(throwOnUnexpectedArg: false);

        private static string _providerName = "memory";
        private static bool _useSnapshots = true;
        private static bool _quietMode = false;

        static void Main(string[] args)
        {
            ParseCommandLine(args);
            
            var store = BuildStore(_providerName);

            using (var app = new SampleApp(store, _providerName, _useSnapshots,_quietMode))
            {
                Console.WriteLine(
                    "Press ENTER to start and wait projections, then press ENTER again to show data & stats.");
                Console.ReadLine();
                app.CreateRooms(32);
                app.AddSomeBookings(200);

                Console.ReadLine();

                app.ShowRooms();
                app.DumpMetrics();

                Console.WriteLine("Press ENTER to close.");
                Console.ReadLine();
            }
        }

        static void ParseCommandLine(string[] args)
        {
            var mongo = cmd.Option("-m|--mongo", "Use mongo as storage", CommandOptionType.NoValue);
            var snapshots = cmd.Option("-s|--snapshots", "Use snapsthos", CommandOptionType.NoValue);
			var quietmode = cmd.Option("-q|--quiet", "Quiet mode", CommandOptionType.NoValue);

			cmd.HelpOption("-? | -h | --help");

            cmd.OnExecute(() =>
            {
                if (mongo.HasValue())
                {
                    _providerName = "mongo";
                }

                _useSnapshots = snapshots.HasValue();
                _quietMode = quietmode.HasValue();
                return 0;
            });
            
            cmd.Execute(args);
        }

        static IRawStore BuildStore(string store)
        {
            Console.WriteLine($"Selected store is {store}");

            switch (store.ToLowerInvariant())
            {
                case "memory":
                {
                    var network = new LocalAreaNetworkSimulator(2, 10);
                    return new InMemoryRawStore(network);
                }

                case "mongo":
                {
                    var options = new MongoStoreOptions
                    {
                        PartitionsConnectionString = Mongo,
                        UseLocalSequence = true,
                        PartitionsCollectionName = "partitions",
                        SequenceCollectionName = "seq",
                        DropOnInit = true,
                        Serializer = new MongoCustomSerializer()
                    };
                    var mongo = new MongoRawStore(options);
                    mongo.InitAsync().GetAwaiter().GetResult();
                    return mongo;
                }
            }

            throw new Exception($"Invalid store {store}");
        }
    }
}