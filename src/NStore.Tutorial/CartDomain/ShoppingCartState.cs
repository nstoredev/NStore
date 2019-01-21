using System.Collections.Generic;
using System.Linq;

namespace NStore.Tutorial.CartDomain
{
    public class ShoppingCartState
    {
        private readonly List<ItemData> _items = new List<ItemData>();

        public int TotalItems => _items.Sum(x=>x.Quantity);
        
        private void On(ItemAddedToCart evt)
        {
            _items.Add(evt.ItemData);
        }
    }
}