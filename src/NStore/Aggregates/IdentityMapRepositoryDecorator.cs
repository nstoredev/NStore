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

        public Task<T> GetById<T>(string id) where T : IAggregate
        {
            return GetById<T>(id, int.MaxValue);
        }

        public Task<T> GetById<T>(string id, CancellationToken cancellationToken) where T : IAggregate
        {
            return GetById<T>(id, int.MaxValue,cancellationToken);
        }

        public async Task<T> GetById<T>(string id, int version, CancellationToken cancellationToken) where T : IAggregate
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

        public Task<T> GetById<T>(string id, int version) where T : IAggregate
        {
            return GetById<T>(id, version, default(CancellationToken));
        }

        public Task Save<T>(T aggregate, string operationId) where T : IAggregate
        {
            return Save<T>(aggregate, operationId, null, default(CancellationToken));
        }

        public Task Save<T>(T aggregate, string operationId, Action<IHeadersAccessor> headers) where T : IAggregate
        {
            return Save<T>(aggregate, operationId, headers, default(CancellationToken));
        }

        public Task Save<T>(
            T aggregate, 
            string operationId, 
            Action<IHeadersAccessor> headers,
            CancellationToken cancellationToken
        ) where T : IAggregate
        {
            return _repository.Save<T>(aggregate, operationId, headers, cancellationToken);
        }
    }
}