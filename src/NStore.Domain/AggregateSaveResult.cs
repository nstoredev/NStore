using System;
using NStore.Core.Persistence;

namespace NStore.Domain
{
    public class AggregateSaveResult
    {
        public string AggregateId { get; set; }
        public bool Succeeded { get; set; }
        public Exception FailureException { get; set; }
        public IChunk Chunk { get; set; }
    }
}