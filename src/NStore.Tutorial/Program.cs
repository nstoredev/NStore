using NStore.Tutorial.CartDomain;
using System;
using System.Threading.Tasks;

namespace NStore.Tutorial
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // runtime
            var runtime = TutorialRuntime.CreateDefaultRuntime();

            // create aggregate
            var repository = runtime.CreateRepository();
            var cartId = Guid.NewGuid().ToString();
            var shoppingCart = await repository.GetByIdAsync<ShoppingCart>(cartId).ConfigureAwait(false);

            // act
            shoppingCart.Add(new ItemData("SKU001", 2, 100));

            await repository.SaveAsync(
                shoppingCart,
                "unique_action_id",
                headers => headers
                    .Add("user", "demo")
                    .Add("timestamp", DateTime.UtcNow)
                    .Add("host", Environment.MachineName)
            ).ConfigureAwait(false);

            // project

            await Task.Delay(500).ConfigureAwait(false);
            Console.WriteLine("Press enter to exit");
            Console.ReadLine();
            runtime.Shutdown();
        }
    }
}