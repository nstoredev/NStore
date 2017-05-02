using System;

namespace NStore.Contracts
{
    public class DuplicateStreamIndexException : Exception
    {
        public long Index { get; }
        public string StreamId { get; }

        public DuplicateStreamIndexException(string streamId, long index) : 
            base($"Duplicated index {index} on stream {streamId}")
        {
            this.Index = index;
            this.StreamId = streamId;
        }
    }
}