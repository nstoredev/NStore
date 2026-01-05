using System;
using NStore.Core.Persistence;

namespace NStore.Domain
{
    public class AggregateSaveResult
    {
        public string AggregateId { get; internal set; }
        public bool Succeeded { get; internal set; }
        public Exception FailureException { get; internal set; }
        public IChunk Chunk { get; internal set; }
    }
}