using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.InMemory;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using Xunit;

namespace NStore.Core.Tests.Persistence
{
    public class PollingClientAcknowledgementTests
    {
        [Fact]
        public async Task transient_consumer_failure_should_not_advance_position()
        {
            using var persistence = await CreatePersistenceAsync(1).ConfigureAwait(false);
            var processingStarted = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            var processing = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var subscription = new LambdaSubscription(_ =>
            {
                processingStarted.SetResult(null);
                return processing.Task;
            });
            var poller = CreatePoller(persistence, 0, subscription);

            var polling = poller.Poll();
            await processingStarted.Task.ConfigureAwait(false);

            Assert.Equal(0, poller.Position);

            processing.SetException(new TransientConsumerException());
            await polling.ConfigureAwait(false);

            Assert.True(subscription.Failed);
            Assert.Equal(0, poller.Position);
        }

        [Fact]
        public async Task transient_consumer_failure_should_retry_in_order_on_same_poller()
        {
            using var persistence = await CreatePersistenceAsync(3).ConfigureAwait(false);
            var deliveries = new List<long>();
            var shouldFail = true;
            var subscription = new LambdaSubscription(chunk =>
            {
                deliveries.Add(chunk.Position);
                if (shouldFail)
                {
                    shouldFail = false;
                    throw new TransientConsumerException();
                }

                return Subscription.Continue;
            });
            var poller = CreatePoller(persistence, 0, subscription);

            await poller.Poll().ConfigureAwait(false);
            await poller.Poll().ConfigureAwait(false);

            Assert.Equal(new long[] { 1, 1, 2, 3 }, deliveries);
            Assert.Equal(3, poller.Position);
        }

        [Fact]
        public async Task restart_from_last_committed_position_should_retry_failed_chunk()
        {
            using var persistence = await CreatePersistenceAsync(2).ConfigureAwait(false);
            var failingSubscription = new LambdaSubscription(_ => throw new TransientConsumerException());
            var failedPoller = CreatePoller(persistence, 0, failingSubscription);

            await failedPoller.Poll().ConfigureAwait(false);
            var lastCommittedPosition = failedPoller.Position;

            var deliveriesAfterRestart = new List<long>();
            var restartedSubscription = new LambdaSubscription(chunk =>
            {
                deliveriesAfterRestart.Add(chunk.Position);
                return Subscription.Continue;
            });
            var restartedPoller = CreatePoller(persistence, lastCommittedPosition, restartedSubscription);

            await restartedPoller.Poll().ConfigureAwait(false);

            Assert.Equal(new long[] { 1, 2 }, deliveriesAfterRestart);
            Assert.Equal(2, restartedPoller.Position);
        }

        [Fact]
        public async Task ambiguous_failure_may_redeliver_with_stable_identity_for_idempotency()
        {
            using var persistence = await CreatePersistenceAsync(1).ConfigureAwait(false);
            var deliveryIdentities = new List<string>();
            var appliedEffects = new HashSet<string>();
            var shouldFailAfterSideEffect = true;
            var subscription = new LambdaSubscription(chunk =>
            {
                var identity = $"{chunk.PartitionId}:{chunk.OperationId}";
                deliveryIdentities.Add(identity);
                appliedEffects.Add(identity);

                if (shouldFailAfterSideEffect)
                {
                    shouldFailAfterSideEffect = false;
                    throw new TransientConsumerException();
                }

                return Subscription.Continue;
            });
            var poller = CreatePoller(persistence, 0, subscription);

            await poller.Poll().ConfigureAwait(false);
            await poller.Poll().ConfigureAwait(false);

            Assert.Equal(2, deliveryIdentities.Count);
            Assert.Single(deliveryIdentities.Distinct());
            Assert.Single(appliedEffects);
            Assert.Equal(1, poller.Position);
        }

        [Fact]
        public async Task cancellation_should_commit_only_acknowledged_chunks_and_resume()
        {
            using var persistence = await CreatePersistenceAsync(3).ConfigureAwait(false);
            using var cancellation = new CancellationTokenSource();
            var deliveries = new List<long>();
            var subscription = new LambdaSubscription(chunk =>
            {
                deliveries.Add(chunk.Position);
                if (chunk.Position == 1)
                {
                    cancellation.Cancel();
                }

                return Subscription.Continue;
            });
            var poller = CreatePoller(persistence, 0, subscription);

            await poller.Poll(cancellation.Token).ConfigureAwait(false);

            Assert.Equal(new long[] { 1 }, deliveries);
            Assert.Equal(1, poller.Position);

            await poller.Poll().ConfigureAwait(false);

            Assert.Equal(new long[] { 1, 2, 3 }, deliveries);
            Assert.Equal(3, poller.Position);
        }

        [Fact]
        public async Task stop_should_acknowledge_current_chunk_and_resume_at_next_chunk()
        {
            using var persistence = await CreatePersistenceAsync(3).ConfigureAwait(false);
            var deliveries = new List<long>();
            var shouldStop = true;
            var subscription = new LambdaSubscription(chunk =>
            {
                deliveries.Add(chunk.Position);
                if (shouldStop)
                {
                    shouldStop = false;
                    return Subscription.Stop;
                }

                return Subscription.Continue;
            });
            var poller = CreatePoller(persistence, 0, subscription);

            await poller.Poll().ConfigureAwait(false);

            Assert.Equal(new long[] { 1 }, deliveries);
            Assert.Equal(1, poller.Position);

            await poller.Poll().ConfigureAwait(false);

            Assert.Equal(new long[] { 1, 2, 3 }, deliveries);
            Assert.Equal(3, poller.Position);
        }

        private static PollingClient CreatePoller(
            InMemoryPersistence persistence,
            long lastPosition,
            ISubscription subscription)
        {
            return new PollingClient(
                persistence,
                lastPosition,
                subscription,
                NStoreNullLoggerFactory.Instance);
        }

        private static async Task<InMemoryPersistence> CreatePersistenceAsync(int eventCount)
        {
            var persistence = new InMemoryPersistence();
            for (var index = 1; index <= eventCount; index++)
            {
                await persistence.AppendAsync(
                        "projection-source",
                        index,
                        $"event-{index}",
                        $"operation-{index}",
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }

            return persistence;
        }

        private sealed class TransientConsumerException : Exception
        {
        }
    }
}
