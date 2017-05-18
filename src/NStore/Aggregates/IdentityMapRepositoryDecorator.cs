using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Aggregates
{
    public class IdentityMapRepositoryDecorator : IRepository
    {
        private readonly IRepository _repository;
        private readonly IDictionary<string, IAggregate> _identityMap = new Dictionary<string, IAggregate>();

        public IdentityMapRepositoryDecorator(IRepository repository)
        {
            _repository = repository;
        }

        public async Task<T> GetById<T>(string id, int version = Int32.MaxValue, CancellationToken cancellationToken = new CancellationToken()) where T : IAggregate
        {
            string mapid = id + "@" + version;
            if (_identityMap.ContainsKey(mapid))
            {
                return (T)_identityMap[mapid];
            }

            var aggregate = await _repository.GetById<T>(id, version, cancellationToken);

            _identityMap.Add(mapid, aggregate);
            return aggregate;
        }

        public Task Save<T>(
            T aggregate, 
            string operationId, 
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = new CancellationToken()
        ) where T : IAggregate
        {
            return _repository.Save<T>(aggregate, operationId, headers, cancellationToken);
        }
    }
}