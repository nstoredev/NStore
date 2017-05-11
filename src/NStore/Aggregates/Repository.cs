using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Raw;
using NStore.Streams;

namespace NStore.Aggregates
{
    public class AggregateReadOnlyException : Exception
    {
    }

    public class Repository : IRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IStreamStore _streams;
        private readonly IDictionary<IAggregate, IStream> _openedStreams = new Dictionary<IAggregate, IStream>();
        private readonly IDictionary<string, IAggregate> _identityMap = new Dictionary<string, IAggregate>();

        public Repository(IAggregateFactory factory, IStreamStore streams)
        {
            _factory = factory;
            _streams = streams;
        }

        public async Task<T> GetById<T>(
            string id,
            int version = Int32.MaxValue,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            string mapid = id + "@" + version;
            if (_identityMap.ContainsKey(mapid))
            {
                return (T) _identityMap[mapid];
            }

            var aggregate = _factory.Create<T>();
            _identityMap.Add(mapid, aggregate);

            aggregate.Init(id);
            var stream = OpenStream(aggregate, version != Int32.MaxValue);
            var persister = (IAggregatePersister) aggregate;

            var consumer = new LambdaConsumer((l, payload) =>
            {
                var commit = (Commit) payload;

                persister.AppendCommit(commit);
                return ScanCallbackResult.Continue;
            });

            await stream.Read(consumer, 0, version, cancellationToken)
                .ConfigureAwait(false);

            return aggregate;
        }

        public async Task Save<T>(
            T aggregate,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default(CancellationToken)
        ) where T : IAggregate
        {
            var stream = GetStream(aggregate);
            if (stream is ReadOnlyStream)
            {
                throw new AggregateReadOnlyException();
            }

            var persister = (IAggregatePersister) aggregate;

            var commit = persister.BuildCommit();

            headers?.Invoke(commit);

            await stream.Append(commit, operationId, cancellationToken).ConfigureAwait(false);
        }

        private IStream OpenStream(IAggregate aggregate, bool isPartialLoad)
        {
            var stream = isPartialLoad
                ? _streams.OpenReadOnly(aggregate.Id)
                : _streams.OpenOptimisticConcurrency(aggregate.Id);

            _openedStreams.Add(aggregate, stream);
            return stream;
        }

        private IStream GetStream(IAggregate aggregate)
        {
            try
            {
                return _openedStreams[aggregate];
            }
            catch (KeyNotFoundException e)
            {
                throw new RepositoryMismatchException();
            }
        }
    }
}