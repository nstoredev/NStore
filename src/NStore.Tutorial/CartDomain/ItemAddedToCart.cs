namespace NStore.Tutorial.CartDomain
{
    /// <summary>
    /// Event raised when an item is added to a shopping cart
    /// </summary>
    public class ItemAddedToCart
    {
        public ItemData ItemData { get; private set; }

        public ItemAddedToCart(ItemData itemData)
        {
            ItemData = itemData;
        }
    }
}