using System.Collections.Generic;
using System.Threading.Tasks;

namespace NStore.Domain.Experimental
{
    public class Recording
    {
        private readonly string _sessionId;
        private readonly IRepository _repository;
        private readonly IList<IAggregate> _tracking = new List<IAggregate>();
        public Recording(string sessionId, IRepository repository)
        {
            _sessionId = sessionId;
            _repository = repository;
        }

        public async Task<PocoAggregate<TState>> GetAsync<TState>(string id) where TState : class, new()
        {
            var aggregate = await _repository.GetByIdAsync<PocoAggregate<TState>>(id).ConfigureAwait(false);
            _tracking.Add(aggregate);
            return aggregate;
        }

        public async Task StreamAsync()
        {
            foreach (var aggregate in _tracking)
            {
                await _repository.SaveAsync(aggregate,_sessionId).ConfigureAwait(false);
            }
        }
    }
}