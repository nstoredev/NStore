using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Persistence;

namespace NStore.InMemory
{
    internal class InMemoryPartition
    {
        private readonly ReaderWriterLockSlim _lockSlim = new ReaderWriterLockSlim();

        public InMemoryPartition(string partitionId, INetworkSimulator networkSimulator, Func<Chunk, Chunk> clone)
        {
            this.Id = partitionId;
            _networkSimulator = networkSimulator;
            Clone = clone;
        }

        private Func<Chunk, Chunk> Clone { get; }
        public string Id { get; set; }
        private IEnumerable<Chunk> Chunks => _byIndex.Values;
        private readonly SortedDictionary<long, Chunk> _byIndex = 
            new SortedDictionary<long, Chunk>();
        private readonly INetworkSimulator _networkSimulator;

        public Task ReadForward(
            long fromLowerIndexInclusive,
            ISubscription subscription,
            long toUpperIndexInclusive,
            int limit,
            CancellationToken cancellationToken)
        {
            _lockSlim.EnterReadLock();

            var result = Chunks
                .Where(x => x.Index >= fromLowerIndexInclusive && x.Index <= toUpperIndexInclusive)
                .Take(limit)
                .ToArray();

            _lockSlim.ExitReadLock();

            return StartProducer(subscription, result, cancellationToken);
        }

        public Task ReadBackward(
            long fromUpperIndexInclusive,
            ISubscription subscription,
            long toLowerIndexInclusive,
            int limit,
            CancellationToken cancellationToken
        )
        {
            _lockSlim.EnterReadLock();

            var result = Chunks.Reverse()
                .Where(x => x.Index <= fromUpperIndexInclusive && x.Index >= toLowerIndexInclusive)
                .Take(limit)
                .ToArray();

            _lockSlim.ExitReadLock();

            return StartProducer(subscription, result, cancellationToken);
        }

        public Task<IChunk> Peek(int maxValue, CancellationToken cancellationToken)
        {
            _lockSlim.EnterReadLock();

            var chunk = Chunks.Reverse()
                .Where(x => x.Index <= maxValue)
                .Take(1)
                .SingleOrDefault();

            _lockSlim.ExitReadLock();


            return Task.FromResult((IChunk) Clone(chunk));
        }

        private async Task StartProducer(
            ISubscription subscription,
            IEnumerable<Chunk> chunks,
            CancellationToken cancellationToken)
        {
            try
            {
                foreach (var chunk in chunks)
                {
                    await _networkSimulator.WaitFast().ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!await subscription.OnNext(Clone(chunk)))
                    {
                        await subscription.Completed();
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                await subscription.OnError(e);
                return;
            }

            await subscription.Completed();
        }


        public void Write(Chunk chunk)
        {
            _lockSlim.EnterWriteLock();
            try
            {
                if (_byIndex.Values.Any(x => x.OpId == chunk.OpId))
                {
                    return;
                }

                if (_byIndex.ContainsKey(chunk.Index))
                {
                    throw new DuplicateStreamIndexException(this.Id, chunk.Index);
                }

                _byIndex.Add(chunk.Index, chunk);
            }
            finally
            {
                _lockSlim.ExitWriteLock();
            }
        }

        public Chunk[] Delete(long fromIndex, long toIndex)
        {
            _lockSlim.EnterReadLock();
            var toDelete = Chunks.Where(x => x.Index >= fromIndex && x.Index <= toIndex).ToArray();
            _lockSlim.ExitReadLock();

            _lockSlim.EnterWriteLock();
            foreach (var chunk in toDelete)
            {
                this._byIndex.Remove(chunk.Index);
            }
            _lockSlim.ExitWriteLock();

            return toDelete;
        }
    }
}