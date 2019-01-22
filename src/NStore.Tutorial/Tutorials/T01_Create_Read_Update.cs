using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using NStore.Domain;
using NStore.Tutorial.CartDomain;

namespace NStore.Tutorial.Tutorials
{
    // ReSharper disable once InconsistentNaming
    public class T01_Create_Read_Update : AbstractTutorial
    {
        protected override async Task RunAsync()
        {
            var cartId = Guid.NewGuid().ToString();
            var repository = CreateRepository();

            // GetByIdAsync create a new Aggregate instance and initialize is state 
            // from the underlying stream (and snapshot if present and valid)
            var cart = await repository.GetByIdAsync<ShoppingCart>(cartId);

            // mutate 
            cart.Add(new ItemData("SKU001", 2, 100));

            // Persist changes as a single "Changeset"
            await repository.SaveAsync(cart, "new");
        }
    }
}