using System.Threading.Tasks;
using NStore.Core.Streams;
using NStore.Tutorial.CartDomain;

namespace NStore.Tutorial.Tutorials
{
    // ReSharper disable once InconsistentNaming
    public class T11_Aggregate_Delete : AbstractTutorial
    {
        protected override async Task RunAsync()
        {
            // Setup
            var repository = CreateRepository();

            var cart = await repository.GetByIdAsync<ShoppingCart>("CART_1");
            cart.Add(new ItemData("SKU",1, 10));
            await repository.SaveAsync(cart, "new");
            
            // Act
            var stream = OpenStream("CART_1");
            await stream.DeleteAsync();
        }
    }
}