#if NET6_0_OR_GREATER
#else
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Core.Tests
{
    public class AsyncParallelExtensionsTests
    {
        [Fact]
        public async Task foreach_async_should_wait_for_running_tasks_before_throwing_cancellation()
        {
            var source = new[] { 1, 2, 3 };
            var firstItemStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirstItem = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var firstItemExited = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var cts = new CancellationTokenSource();

            var execution = AsyncParallelExtensions.ForEachAsync(
                source,
                maxDegreeOfParallelism: 1,
                async (item, _) =>
                {
                    if (item == 1)
                    {
                        firstItemStarted.TrySetResult(true);
                        await releaseFirstItem.Task.ConfigureAwait(false);
                        firstItemExited.TrySetResult(true);
                    }
                },
                cts.Token);

            await firstItemStarted.Task.ConfigureAwait(false);
            cts.Cancel();

            Assert.False(firstItemExited.Task.IsCompleted);
            Assert.False(execution.IsCompleted);

            releaseFirstItem.TrySetResult(true);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await execution.ConfigureAwait(false));
        }

        [Fact]
        public async Task foreach_async_should_fail_fast_on_first_exception()
        {
            var source = new[] { 1, 2, 3 };

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await AsyncParallelExtensions.ForEachAsync(
                        source,
                        maxDegreeOfParallelism: 3,
                        (item, _) =>
                        {
                            if (item % 2 == 0)
                            {
                                throw new InvalidOperationException($"boom-{item}");
                            }

                            return Task.CompletedTask;
                        },
                        CancellationToken.None)
                    .ConfigureAwait(false))
                .ConfigureAwait(false);

            Assert.Equal("boom-2", error.Message);
        }

        [Fact]
        public async Task foreach_async_should_include_cancellation_when_worker_failure_cancels_running_work()
        {
            var source = new[] { 1, 2 };
            var firstItemStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var error = await Assert.ThrowsAsync<AggregateException>(async () =>
                await AsyncParallelExtensions.ForEachAsync(
                        source,
                        maxDegreeOfParallelism: 2,
                        async (item, ct) =>
                        {
                            if (item == 1)
                            {
                                firstItemStarted.TrySetResult(true);
                                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
                                return;
                            }

                            await firstItemStarted.Task.ConfigureAwait(false);
                            throw new InvalidOperationException("boom");
                        },
                        CancellationToken.None)
                    .ConfigureAwait(false))
                .ConfigureAwait(false);

            Assert.Contains(error.InnerExceptions, ex => ex is InvalidOperationException invalid && invalid.Message == "boom");
            Assert.Contains(error.InnerExceptions, ex => ex is OperationCanceledException);
        }
    }
}
#endif