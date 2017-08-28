using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Domain
{
    public interface IRepository
    {
        Task<T> GetById<T>(
            string id
        ) where T : IAggregate; 
        
        Task<T> GetById<T>(
            string id,
            CancellationToken cancellationToken
        ) where T : IAggregate;    
        
        Task Save<T>(
            T aggregate, 
            string operationId
        ) where T : IAggregate;  
        
        Task Save<T>(
            T aggregate, 
            string operationId,
            Action<IHeadersAccessor> headers
        ) where T : IAggregate;      
        
        Task Save<T>(
            T aggregate, 
            string operationId,
            Action<IHeadersAccessor> headers,
            CancellationToken cancellationToken
        ) where T : IAggregate;
    }
}