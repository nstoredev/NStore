# Design principles

## API
NStore implements persistence API at three different levels.

1. [Raw Persistence](#raw-persistence-layer-0)
1. [Streams](#streams-layer-1)
1. [Domain](#domain-layer-2)


## Raw Persistence (Level 0)
Raw persistence are `Facade` over native database drivers to provide a common API for Chunk and Store operations.

Every NStore persistence:
1. implements a globally ordered sequence of `Chunks`.
2. can be paritioned in smaller sequences of `Chunks` with custom ordering
3. supports idempotency on write


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

## Streams (Level 1)

A Stream is an ordered sequence of zero or more Chunks. Streams are mapped over Partitions of Layer 0. 

`PartitionId -> StreamId`

## Domain (Level 2)

Domain add support for event sourced `Aggregate` persisted as a `Stream` of `Changeset` payloads. 

`PartitionId -> StreamId -> AggregateId`