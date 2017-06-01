using System.Collections.Generic;
using System.Linq;
using NStore.Persistence;

namespace NStore.InMemory
{
    internal class InMemoryPartition
    {
        public InMemoryPartition(string partitionId)
        {
            this.Id = partitionId;
        }

        public string Id { get; set; }
        public IEnumerable<Chunk> Chunks => _byIndex.Values.OrderBy(x => x.Index);
        private readonly IDictionary<long, Chunk> _byIndex = new Dictionary<long, Chunk>();

        public void Write(Chunk chunk)
        {
            if (_byIndex.Values.Any(x => x.OpId == chunk.OpId))
            {
                return;
            }

            if (_byIndex.ContainsKey(chunk.Index))
                throw new DuplicateStreamIndexException(this.Id, chunk.Index);

            _byIndex.Add(chunk.Index, chunk);
        }

        public Chunk[] Delete(long fromIndex, long toIndex)
        {
            var toDelete = Chunks.Where(x => x.Index >= fromIndex && x.Index <= toIndex).ToArray();

            foreach (var chunk in toDelete)
            {
                this._byIndex.Remove(chunk.Index);
            }

            return toDelete;
        }
    }
}