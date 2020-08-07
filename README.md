<img src="logo/logo.png" alt="logo" height="120" align="right" />

# NStore

**(Yet Another) Opinionated Event Sourcing Library**

This project is a playground for experimenting with .net Standard, async and a simple API for a Sql/NoSql backed EventStore.
Heavily inspired from NEventStore, rewritten from scratch to be simple to learn and highly extensible.

## CI Status

| Build server | Platform | Build Status                                                                                                                                                           |
| ------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AppVeyor     | Windows  | [<img src="https://ci.appveyor.com/api/projects/status/github/proximosrl/nstore?svg=true" alt="Build status" >](https://ci.appveyor.com/project/andreabalducci/nstore) |
| Travis       | Ubuntu   | [<img src="https://travis-ci.org/ProximoSrl/NStore.svg?branch=develop" alt="Build status" >](https://travis-ci.org/ProximoSrl/NStore)                                  |
| GH Actions   | Linux    | [<img src="https://github.com/ProximoSrl/NStore/workflows/NStore%20CI/badge.svg" alt="Build status" >](https://github.com/ProximoSrl/NStore/blob/develop/.github/workflows/ci.yml)                                  |
| Azdo         | Windows  | [<img src="https://dev.azure.com/gianmariaricci/Public/_apis/build/status/130" alt="Build status" >](https://dev.azure.com/gianmariaricci/Public/_build/latest?definitionId=130)                                  |

## Quickstart

### Streams API

    var persister = new InMemoryPersistence();
    var streams = new StreamsFactory(persister);

    // Write to stream
    var stream = streams.Open("Stream_1");
    await stream.AppendAsync(new { data = "Hello world!" });

    // Read from stream
    await stream.ReadAsync(data => {
        Console.WriteLine($"  index {data.Index} => {data.Payload}");
        return Task.FromResult(true);
    });

---

## Learn

The source comes with a [Sample App](https://github.com/ProximoSrl/NStore/tree/develop/src/NStore.Sample) to illustrate some basic stuff you can do.

## Status

In production (MongoDB & SqlServer) since 2017 on two major products.
