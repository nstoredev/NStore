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


        void Save<T>(T aggregate) where T : IAggregate;
    }
}