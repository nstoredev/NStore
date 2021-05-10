using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Processing;
using NStore.Core.Streams;

namespace NStore.Quickstart
{
    static class Program
    {
        /// <summary>
        /// Our tracked event
        /// </summary>
        public record Favorited(string UserId, DateTime When);

        /// <summary>
        /// Counts unique favorites
        /// </summary>
        public class UniqueFavs
        {
            private readonly HashSet<string> _users = new();

            // ReSharper disable once UnusedMember.Local
            private void On(Favorited fav) => _users.Add(fav.UserId);

            public int Count => _users.Count;
        }

        static async Task Main()
        {
            // StreamsFactory setup
            var streams = new StreamsFactory(new InMemoryPersistence());

            // Open the stream in r/w
            var post = streams.Open("post/123");
            
            // Write to stream
            await post.AppendAsync(new Favorited("users/200", DateTime.UtcNow));
            await post.AppendAsync(new Favorited("users/404", DateTime.UtcNow));
            await post.AppendAsync(new Favorited("users/200", DateTime.UtcNow));

            // Read the stream from start
            await post.ReadAsync(chunk =>
            {
                Console.WriteLine($"{chunk.PartitionId} #{chunk.Index} => {chunk.Payload}");
                return Subscription.Continue;
            });

            // Stream processing
            var favs = await post.AggregateAsync<UniqueFavs>();
            Console.WriteLine($"{favs.Count} users added '{post.Id}' as favorite");


            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }
    }

}
