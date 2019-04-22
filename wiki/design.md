# Design principles

## API
NStore implements persistence API at three different levels.

1. [Raw Persistence](#raw-persistence-level-0)
1. [Streams](#streams-level-1)
1. [Domain](#domain-level-2)


## Raw Persistence (Level 0)
Raw persistence are `Facade` over native database drivers to provide a common API for Chunk and Store operations.

Every NStore persistence:
1. implements a globally ordered sequence of `Chunks`.
2. can be paritioned in smaller sequences of `Chunks` with custom ordering
3. supports write idempotency

### Storage Layout

|                   | 1   | 2   | 3  | 4  | 5  | 6  | 7  | Position |
| :-                | -:  | -:  | -: | -: | -: | -: | -: | :-       |
| Users/1           | 1   |     |    |    |  2 |  3 |    |          |
| Users/1/Clicks    |     |     | 10 |    |    |    | 20 |          |
| Users/2           |     | 1   |    | 2  |    |    |    |          |
| **Partition**|

Every cell represents a `Chunk` idenfied by the `Partition Index` assigned by the caller. 
Store can be read ordered by global `Position` or by single streams ordered by `Index`.

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

#### Logical mapping
`PartitionId -> StreamId`

## Domain (Level 2)

Domain add support for event sourced `Aggregate` persisted as a `Stream` of `Changeset` payloads. 

#### Logical mapping

`PartitionId -> StreamId -> AggregateId`