using System;
using System.Threading.Tasks;
using NStore.Domain;
using NStore.Tutorial.CartDomain;
using NStore.Tutorial.Support;

namespace NStore.Tutorial
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // runtime
            var runtime =  ApplicationRuntime.CreateDefaultRuntime();

            // create aggregate
            var repository = runtime.CreateRepository();
            var cartId = Guid.NewGuid().ToString();
            var shoppingCart = await repository.GetByIdAsync<ShoppingCart>(cartId);
               
            // act
            shoppingCart.AddToBasket(new ItemData("SKU001", 2, 100));

            // project
            
            Console.WriteLine("Press enter to exit");
            Console.ReadLine();
            runtime.Shutdown();
        }
    }
}