## 1.2.0

- **Breaking (delivery semantics):** `PollingClient` now advances its position only **after** `ISubscription.OnNextAsync` completes successfully. A faulted or cancelled `OnNextAsync` no longer advances the checkpoint, so the chunk is retained and redelivered on the next poll (and after a restart from the last acknowledged position). Previously the position was advanced *before* dispatch, which permanently skipped any chunk whose consumer threw.
- **Behavioral change to be aware of when upgrading:** consumers that previously threw from `OnNextAsync` had that chunk silently skipped and processing continued; they will now see the chunk retried instead. Delivery is **at least once** — make side effects idempotent using a stable chunk identity. A chunk that always fails will be retried indefinitely (there is no dead-letter/max-retry for consumer failures, unlike the existing five-attempt hole-skip policy), so a permanently failing chunk blocks the projection and re-invokes `OnErrorAsync` each poll.
- `Subscription.Stop` semantics are unchanged: the current chunk is acknowledged, then polling stops before the next chunk.
- `ISubscription` documentation updated to describe the acknowledgement contract; `OnErrorAsync` now explicitly states the failed chunk is not acknowledged and durable checkpoints must not be advanced from that callback.

## 1.1.0

- Added synchronous Mongo persistence initialization support with `Init()`, plus safer Mongo cleanup via `DropAsync()` when collection drop permissions are limited.
- Improved cancellation handling across persistence implementations so cancelled subscriptions stop cleanly instead of surfacing errors.
- Removed LiteDB persistence and related tests/build packaging from the solution.

## 0.21.0

- Introduced BatchRepository for batch operations.
- Improved the interface for IPersistence to support batch operations.

## 0.20.2

- Fixed a potential memory Leak for missing dispose in CancellationTokenSource and ReaderWriterLockSlim.
- Fix usage of ReadWriteLockSlim to avoid potential deadlocks.


## 0.20.1

- Upgraded MongoDb Driver to 3.4.2.

## 0.20.0

- Upgraded MongoDb Driver to 3.1.0.

## 0.19.0

- Upgraded MongoDb Driver to 2.30 to easy transition to V3 https://github.com/mongodb/mongo-csharp-driver/releases/tag/v2.30.0

## 0.18.3

- Updated workflow for CI (uses v2 of the upload artifacts that was deprecated.)

## 0.18.2

- Updated mongodb driver to 2.28 (first version that is signed)

## 0.18.1

- Better error message when race position happens.

## 0.18.0

- Updated Testing libraries.
- Updated System.Data.SqlClient
- Updated MongodbDrivers
- Centralized package management for nuget.
- Fixed build and symbol indexing.

## 0.17.0

- Updated Mongodb Drivers to 2.23.1
- Updated some basic dependencies.

## 0.16.1

- Updated Mongodb driver to 2.21
- Updated libraries for test (xunit, test runner)

## 0.16.0

- Externalized creation of Mongodb client to allow the creation of a single MongoClient

## 0.15.1

- Forced Mongodb drivers to use LINQ 2 provider due to excessive number of bugs for LINQ 3
- Added support for symbol server outside of azure devops with internal builds.

## 0.14-0.15

- Mostly driver updates.

## 0.13.0

- Added multi partition read.

## 0.12.1

- Fixed github action CI.

## 0.12.0

- Updated references
- Bumped full framework to 4.8

## 0.11.x

- Added ability to use a readonly connection on MongoDb persistence layer.

### Breaking Changes

- IPersistence does not support anymore the AppendAsync method, you should use a Stream class to have this behavior. This is part of removing the support of IPersistence for generation of Index value.

## 0.10.4

- Disabled parallelism on tests.

## 0.10.3

- Updated mongodb driver to latest version

## 0.10.2

- Updated mongodb driver to latest version

## 0.10.1

- Updated mongodb driver to latest version
- Fixed cake build.

## 0.9.0

- Minor fixes

## 0.8.2

- Fix wrong merge.

## 0.8.1

- Updated mongodb Driver 
- Updated to latest version of Newtonsoft.Json

## 0.8.0

- Minor modification to Sql Persistence.

## ...
