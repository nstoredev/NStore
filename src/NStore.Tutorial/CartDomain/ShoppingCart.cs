using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NStore.Domain;

namespace NStore.Tutorial.CartDomain
{
    public class ShoppingCart : Aggregate<ShoppingCartState>
    {
        private readonly ILogger<ShoppingCart> _logger;

        public ShoppingCart(ILogger<ShoppingCart> logger)
        {
            _logger = logger;
            _logger.LogDebug("This is an unitilized shopping cart, will be initialized by the Repository.");
        }

        protected override void AfterInit()
        {
            _logger.LogDebug(IsNew
                ? $"This is a brand new shopping cart with id '{this.Id}'"
                : $"This shopping cart has been restored from the stream '{this.Id}'");
        }

        public void Add(ItemData itemData)
        {
            _logger.LogDebug($"Adding {itemData.Quantity} items to this cart");
            DiagnosticEmit(new ItemAddedToCart(itemData));
            _logger.LogDebug($"This cart is now composed of {State.TotalItems} items");
        }

        private void DiagnosticEmit(object evt)
        {
            var json = JsonConvert.SerializeObject(evt, Formatting.Indented);
            var evtType = evt.GetType().Name;

            _logger.LogTrace("Current state: {state}", State);
            
            // Events are routed by the payload processor instance configured in ctor.
            // Default payload processor route events by convention to 
            // private void On(EventType evt) methods in the State object
            Emit(evt);

            _logger.LogTrace("Emitted event {evt}: {data}", evtType, json);
            _logger.LogTrace("State after event {evt}: {state}", evtType, State);
        }
    }
}