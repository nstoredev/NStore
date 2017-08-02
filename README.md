<img src="logo/logo.png" alt="logo" height="120" align="right" />

# NStore
[<img src="https://ci.appveyor.com/api/projects/status/github/proximosrl/nstore?svg=true" alt="Project Badge" >](https://ci.appveyor.com/project/andreabalducci/nstore) 


## Introduction
This project is a playground for experimenting with .net Standard, async and a simple API for a Sql/NoSql backed EventStore.

Heavily inspired from NEventStore, rewritten from scratch to be simple to learn and highly extensible.


---
## Quickstart

### Streams API

    var persister = CreateYourStore();
    var streams = new StreamsFactory(persister);

    // Write to stream
    var stream = streams.Open("Stream_1");
    await stream.AppendAsync(new { data = "Hello world!" });

    // Read from stream
    await stream.ReadAsync(data => {
        Console.WriteLine($"  index {data.Index} => {data.Payload}");
        return Task.FromResult(true);
    });

### Raw API

    var persister = CreateYourStore(); 
    await persister.Append("Stream_1", new { data = "Hello world!" });


---
## Learn
The source comes with a [Sample App](https://github.com/ProximoSrl/NStore/tree/develop/src/NStore.Sample) to illustrate some basic stuff you can do.

## Follow this project
[Roadmap](https://github.com/ProximoSrl/NStore/milestones?direction=asc&sort=due_date&state=open)
