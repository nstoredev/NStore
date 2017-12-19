using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Streams;

namespace NStore.Quickstart
{
    static class Program
    {
        static void Main(string[] args)
        {
            streams_api().GetAwaiter().GetResult();
            raw_api().GetAwaiter().GetResult();

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static async Task streams_api()
        {
            var persister = CreateYourStore();
            var streams = new StreamsFactory(persister);

            Console.WriteLine("Writing to Stream_1");
            var stream = streams.Open("Stream_1");
            await stream.AppendAsync(new { data = "Hello world!" }).ConfigureAwait(false);

            Console.WriteLine("Reading from Stream_1");
            await stream.ReadAsync(data =>
            {
                Console.WriteLine($"  index {data.Index} => {data.Payload}");
                return Task.FromResult(true);
            }).ConfigureAwait(false);
        }

        private static async Task raw_api()
        {
            var persister = CreateYourStore();
            await persister.AppendAsync("Stream_1", new { data = "Hello world!" }).ConfigureAwait(false);
        }

        private static IPersistence CreateYourStore()
        {
            return new InMemoryPersistence();
        }
    }
}
