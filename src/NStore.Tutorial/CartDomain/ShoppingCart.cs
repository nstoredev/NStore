using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NStore.Domain;

namespace NStore.Tutorial.CartDomain
{
    /// <summary>
    /// Shopping cart aggregate.
    /// Reacts to external "commands" emitting events.
    /// </summary>
    public class ShoppingCart : Aggregate<ShoppingCartState>, IInvariantsChecker
    {
        private readonly ILogger<ShoppingCart> _logger;

        public ShoppingCart(ILogger<ShoppingCart> logger)
        {
            _logger = logger;
            _logger.LogDebug("This is an uninitialized shopping cart, will be initialized by the Repository.");
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
            _logger.LogDebug($"This cart is now composed of {State.NumberOfItems} items");
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

        /// <summary>
        /// Opt-in invariants check. Only valid aggregates are persisted 
        /// </summary>
        /// <returns>Check result</returns>
        public InvariantsCheckResult CheckInvariants()
        {
            if (State.NumberOfItems > 0)
            {
                return InvariantsCheckResult.Ok;
            }
            
            return InvariantsCheckResult.Invalid("Shopping cart cannot be empty");
        }
    }
}