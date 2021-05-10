using System.Threading.Tasks;

namespace NStore.Core.Persistence
{
    /// <summary>
    /// Subscription 
    /// </summary>
    public static class Subscription
    {
        /// <summary>
        /// Subscription should continue
        /// </summary>
        public static readonly Task<bool> Continue = Task.FromResult(true);

        /// <summary>
        /// Subscription must stop
        /// </summary>
        public static readonly Task<bool> Stop = Task.FromResult(false);
    }
}