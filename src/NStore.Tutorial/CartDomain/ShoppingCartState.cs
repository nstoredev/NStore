using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace NStore.Tutorial.CartDomain
{
    /// <summary>
    /// Internal state of the ShoppingCart Aggregate.
    /// State separated from aggregate allow:
    /// - Automatic snapshots
    /// - Clean separation of responsibility
    /// - State Projections for "real time reads" outside an aggregate
    ///
    /// State should be mutated only by events
    /// Events are routed by conventions (routing can be customized in the aggregate)
    /// on private void On(EventType evt) method.
    ///
    /// Events handling is opt-in, some events could not be interesting for the
    /// state, just ignore them.
    ///
    /// State should be read by public properties, expressed in ubiquitous language.
    /// 
    /// </summary>
    public class ShoppingCartState
    {
        private readonly List<ItemData> _items = new List<ItemData>();

        //
        // READ
        //
        public int NumberOfItems => _items.Sum(x=>x.Quantity);
        public decimal TotalAmount => _items.Sum(x=>x.Quantity * x.UnitPrice);

        //
        // MUTATE
        //
        
        /// <summary>
        /// Mutate state in response of a <see cref="ItemAddedToCart"/> event
        /// </summary>
        /// <param name="evt">Event payload</param>
        private void On(ItemAddedToCart evt)
        {
            _items.Add(evt.ItemData);
        }

        //
        // Diagnostics
        // 
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }
    }
}