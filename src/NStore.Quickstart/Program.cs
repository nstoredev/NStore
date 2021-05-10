using System;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Streams;

namespace NStore.Quickstart
{
    static class Program
    {
        public record Favorited(string UserId, DateTime When);

        static async Task Main()
        {
            // StreamsFactory setup
            var streams = new StreamsFactory(new InMemoryPersistence());

            // Open the stream in r/w
            var post = streams.Open("post/123/favs");
            
            // Write to stream
            await post.AppendAsync(new Favorited("users/200", DateTime.UtcNow));
            await post.AppendAsync(new Favorited("users/404", DateTime.UtcNow));

            // Read the stream from start
            await post.ReadAsync(chunk =>
            {
                Console.WriteLine($"{chunk.PartitionId} #{chunk.Index} => {chunk.Payload}");
                return Subscription.Continue;
            });

  
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }
    }
}
