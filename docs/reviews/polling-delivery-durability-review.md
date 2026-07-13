# Polling delivery durability review

Date: 2026-07-13

## Outcome

NStore had a confirmed `PollingClient` defect: the internal position advanced before the
subscription task completed. Persistence providers report subscription exceptions through
`OnErrorAsync`. If that callback returned normally, the next poll started after the failed
chunk and could lose projection work permanently.

The fix commits `PollingClient.Position` only after `OnNextAsync` completes successfully.
The existing meaning of `Subscription.Stop` is preserved: it acknowledges the current chunk
and stops before the next one.

The integrating incident that prompted this review had a separate root cause. Its consumer
marked downstream work as failed, skipped a required payload write, and nevertheless completed
normally and saved its checkpoint. NStore cannot distinguish that deliberate acknowledgement
from successful processing. Hole detection cannot recover an acknowledged position.

## Delivery and persistence guarantees

- A successful `AppendAsync` means the NStore persistence operation completed. NStore has no
  generic post-commit dispatcher and does not include downstream projection success in the
  append result.
- Normal completion of `ISubscription.OnNextAsync` acknowledges the current chunk. `true`
  continues; `false` acknowledges the current chunk and stops.
- A faulted or cancelled subscription task does not acknowledge the chunk in `PollingClient`.
  The live poller retains its prior position, and a later poll can redeliver the failed chunk.
- `PollingClient` keeps its checkpoint in memory. After restart, the application must construct
  it with the last durably acknowledged position.
- Delivery is at least once, not exactly once. A consumer can finish a side effect and then fail
  before acknowledgement, causing duplicate delivery. Consumers must use a stable identity such
  as `(PartitionId, OperationId)` and make side effects idempotent.
- Returning success after skipping required work is an acknowledgement. NStore cannot inspect a
  consumer's private failure flags, downstream writes, or external checkpoint transaction.
- Cancellation stops the current read. Chunks whose callbacks completed successfully remain
  acknowledged; later chunks resume from the retained position.
- Hole detection is an ordering aid, not failure detection. The current compatibility behavior
  retries a missing global position five times and can then skip a persistently absent position.
  It cannot recreate or reject a position that a consumer already acknowledged.

## Decisions and rejected alternatives

1. **Commit the position after successful callback completion.** This is the smallest change
   that restores at-least-once polling after transient consumer exceptions.
2. **Keep the synchronous completion fast path.** Most subscriptions return the cached
   `Subscription.Continue` task. That path updates state and returns a cached task without an
   async state-machine allocation. Genuinely asynchronous callbacks use a small await helper.
3. **Do not reinterpret `Subscription.Stop` as a negative acknowledgement.** Existing callers
   use it to process one chunk successfully and stop. Changing it would break checkpoint and
   resume behavior. Consumers that need retry must fault or cancel the callback.
4. **Do not add MongoDB-specific retry or telemetry logic.** The defect is above the provider
   boundary and reproduces with `InMemoryPersistence`. The fix uses only `ISubscription` and is
   independent of MongoDB server and driver releases.
5. **Do not add a durable checkpoint store or downstream result type in this fix.** NStore does
   not own consumer side effects or their transaction. Such an API would expand the abstraction
   without preventing a consumer from falsely reporting success.
6. **Do not fold post-commit dispatch into `AppendAsync`.** Persistence durability and downstream
   delivery are different transactions. Applications that need durable fan-out should poll the
   persisted log or use a transactional outbox and acknowledge only after required durable work.

## Deterministic evidence

Before the production change, the six focused acknowledgement tests produced two passes and four
expected failures:

- failure advanced `Position` from `0` to `1`;
- same-poller retry delivered `1,2,3` instead of `1,1,2,3`;
- restart from the retained checkpoint delivered only `2` instead of `1,2`;
- an ambiguous side-effect failure delivered once instead of demonstrating at-least-once
  duplicate delivery.

After the change, all six tests pass. They cover transient failure, same-poller retry, restart,
ordering, duplicate delivery and idempotent application, cancellation, and the acknowledgement
meaning of `Subscription.Stop`. They use no sleeps, external services, unstable driver telemetry,
or absolute timing.

Final verification:

- `dotnet test NStore.Core.Tests ... --filter *PollingClientAcknowledgementTests*`: 6 passed;
- full `NStore.Core.Tests` on `net10.0`: 111 passed;
- SQLite poller, hole, and cancellation contract tests on `net10.0`: 8 passed;
- multi-target `NStore.Core.Tests` Release build (`net6.0` and `net10.0`): 0 errors and
  0 warnings;
- full 18-project Release solution build: 0 errors. The restored dependency graph reports
  existing package vulnerability and compatibility warnings unrelated to this change.

The machine has no .NET 6 runtime, so `net6.0` was compile-verified but not executed. The first
full solution build was intentionally attempted with `--no-restore` and failed only because this
fresh worktree lacked assets files for untouched projects. `dotnet restore` generated them, and
the same build then passed.

## MongoDB compatibility

The MongoDB provider already awaits `FindAsync`/cursor operations with cancellation and routes
read or consumer exceptions to `OnErrorAsync`. Current official MongoDB C# driver sources classify
network and paused-pool failures as retryable read failures. This NStore change does not classify
driver exceptions, inspect internal telemetry, alter filters, or change cursor usage. It therefore
remains compatible with current and future server/driver behavior as long as the provider preserves
the public `ISubscription` contract.

## Hot-path measurement

A task-scoped BenchmarkDotNet 0.15.8 harness compared the old advance-before-callback
sequence with the new advance-after-callback sequence on .NET 10.0.9, Apple M5 Pro. The
harness isolates the acknowledgement branch; it is not a persistence or end-to-end benchmark.

| Callback completion | Old mean | New mean | Old allocation | New allocation |
| --- | ---: | ---: | ---: | ---: |
| Cached synchronous task | 0.4871 ns | 0.5087 ns | 0 B | 0 B |
| Forced asynchronous task | 1.121 μs | 1.095 μs | 96 B | 214 B |

The synchronous path, which includes `Subscription.Continue` and `Subscription.Stop`, remains
allocation-free. Its measured delta was 0.0216 ns (about 4%). Asynchronous timing was within the
run's variance; the 118-byte increase is the await continuation required to commit the position
after the callback finishes. The temporary benchmark project and artifacts were kept outside the
repository in `/private/tmp`.

## Residual risks

- A consumer that catches a transient failure and returns normally will still lose work by
  contract. NStore has no generic signal that the acknowledgement is false.
- Side effects and external checkpoint writes are not atomic with NStore reads. Consumers must
  be idempotent and persist checkpoints only after required durable side effects.
- `OnErrorAsync` controls policy. If it throws, a manually invoked `Poll` propagates the error and
  the background polling task stops; the retained position still allows a later restart to retry.
- The five-attempt hole-skip policy remains unchanged for compatibility and can skip a position
  that appears only after the retry window. This is distinct from consumer acknowledgement and
  warrants a separate design review if deployments can expose long-lived temporary holes.
- The synchronous callback path avoids new allocations, but the asynchronous path necessarily
  adds one await state machine so acknowledgement can occur after completion.
