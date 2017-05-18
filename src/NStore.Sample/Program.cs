using System;
using NStore.InMemory;
using NStore.Persistence.Mongo;
using NStore.Raw;
using NStore.Sample.Support;

namespace NStore.Sample
{
    class Program
    {
        private static string Mongo = "mongodb://localhost/NStoreSample";
        
        static void Main(string[] args)
        {
            var store = BuildStore(args.Length > 0 ? args[0] : "memory");

            using (var app = new SampleApp(store))
            {
                Console.WriteLine(
                    "Press ENTER to start and wait projections, then press ENTER again to show data & stats.");
                Console.ReadLine();
                app.CreateRooms(2);
                app.AddSomeBookings(10);

                Console.ReadLine();

                app.ShowRooms();
                app.DumpMetrics();

                Console.WriteLine("Press ENTER to close.");
                Console.ReadLine();
            }
        }

        static IRawStore BuildStore(string store)
        {
            Console.WriteLine($"Selected store is {store}");

            switch (store.ToLowerInvariant())
            {
                case "memory":
                {
                    var network = new LocalAreaNetworkSimulator(10, 50);
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