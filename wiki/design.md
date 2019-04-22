# Design principles

## API
NStore implements persistence API at three different levels.

1. Raw Persistence
1. Streams
1. Domain


## Raw Persistence (Layer 0)
Every NStore persistence:
1. implements a globally ordered sequence of `Chunks`.
2. can be paritioned in smaller sequences of `Chunks` with custom ordering
3. supports idempotency on write

Raw persistence are `Facade` for native storage drivers.

### Chunk
Chunks are the storage building blocks.  

```csharp
public interface IChunk
{
    long Position { get; }
    string PartitionId { get; }
    long Index { get; }
    object Payload { get; }
    string OperationId { get; }
}
```

**Position**

Global unique position number.


**PartitionId**

Allow correlation between Chunks (eg: streams / aggregates)


**Index**

Chunk index relative to the `PartitionId`

**Payload**

Data payload 

**OperationId**

Tracks the write operation identifier, OperationId is guaranteed to be unique in a given Partition

## Streams (Layer 1)

tbd


## Domain (Layer 2)

tbd