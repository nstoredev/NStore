using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public interface IRepository
    {
        Task<T> GetById<T>(
            string id,
            int version = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate;

        Task Save<T>(
            T aggregate, 
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate;
    }
}