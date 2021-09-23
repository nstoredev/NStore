<img src="logo/logo.png" alt="logo" height="120" align="right" />

# NStore

## (Yet Another) Opinionated Event Sourcing Library

NStore started as a playground for experimenting with .net Standard, async and a simple API for a Sql/NoSql backed EventStore.

After years of experience running NEventStore in production we wrote NStore from scratch with a simpler and extensible API.

## CI Status

| Build server | Platform | Build Status                                                                                                                                                           |
| ------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AppVeyor     | Windows  | [<img src="https://ci.appveyor.com/api/projects/status/github/proximosrl/nstore?svg=true" alt="Build status" >](https://ci.appveyor.com/project/andreabalducci/nstore) |                                 |
| GH Actions   | Linux    | [<img src="https://github.com/ProximoSrl/NStore/workflows/NStore%20CI/badge.svg" alt="Build status" >](https://github.com/ProximoSrl/NStore/blob/develop/.github/workflows/ci.yml)                                  |
| Azdo         | Windows  | [<img src="https://dev.azure.com/gianmariaricci/Public/_apis/build/status/130" alt="Build status" >](https://dev.azure.com/gianmariaricci/Public/_build/latest?definitionId=130)                                  |

## Quickstart

Setup the streams factory

```csharp
var streams = new StreamsFactory(new InMemoryPersistence());
```
open the stream
```csharp
var post = streams.Open("post/123");
```
append new data
```csharp
await post.AppendAsync(new Favorited("users/200", DateTime.UtcNow));
```
read the stream
```csharp
await post.ReadAsync(chunk =>
{
    Console.WriteLine($"{chunk.PartitionId} #{chunk.Index} => {chunk.Payload}");
    return Subscription.Continue;
});
```

Process the stream
```csharp
var favs = await post.AggregateAsync<UniqueFavs>();

Console.WriteLine($"{favs.Count} users added '{post.Id}' as favorite");

```

Full source at [src/NStore.Quickstart/Program.cs](src/NStore.Quickstart/Program.cs)

---

## Learn

The source comes with a [Sample App](https://github.com/ProximoSrl/NStore/tree/develop/src/NStore.Sample) to illustrate some basic stuff you can do.

## Status

In production (MongoDB & SqlServer) since 2017 on two major products.
