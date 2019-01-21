using System;
using Microsoft.Extensions.Logging;
using NStore.Domain;

namespace NStore.Tutorial.CartDomain
{
    public class ShoppingCart : Aggregate<ShoppingCartState>
    {
        private readonly ILogger<ShoppingCart> _logger;

        public ShoppingCart(ILogger<ShoppingCart> logger)
        {
            _logger = logger;
            _logger.LogDebug("This is an empty shopping cart, should be initialized before use.");
        }

        protected override void AfterInit()
        {
            _logger.LogDebug(IsNew
                ? $"This is a brand new shopping cart with id '{this.Id}'"
                : $"This shopping cart has been restored from the stream '{this.Id}'");
        }

        public void AddToBasket(ItemData itemData)
        {
            _logger.LogDebug($"Adding {itemData.Quantity} items to this cart");
            Emit(new ItemAddedToCart(itemData));
            _logger.LogDebug($"This cart is now composed of {State.TotalItems} items");
        }
    }
}