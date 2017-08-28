using System;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using Xunit;

namespace NStore.Core.Tests.Persistence
{
    public class LambdaSubscriptionTests
    {
        private readonly ChunkProcessor _empty = (chunk => Task.FromResult(true));

        [Fact]
        public void new_subscription_should_be_healthy()
        {
            var subscription = new LambdaSubscription(_empty);

            Assert.False(subscription.ReadCompleted);
            Assert.False(subscription.Failed);
            Assert.Null(subscription.LastError);
            Assert.Equal(0, subscription.FailedPosition);
        }

        [Fact]
        public async Task should_track_errors()
        {
            var subscription = new LambdaSubscription(_empty);

            var ex = new Exception("message");
            await subscription.OnErrorAsync(1, ex);

            Assert.True(subscription.Failed);
            Assert.Same(ex, subscription.LastError);
            Assert.Equal(1, subscription.FailedPosition);
        }

        [Fact]
        public async Task should_intercept_on_error()
        {
            Exception trackedException = null;
            long trackedPosition = 0;
            var subscription = new LambdaSubscription(_empty)
            {
                OnError = (position, exception) =>
                {
                    trackedException = exception;
                    trackedPosition = position;
                    return Task.CompletedTask;
                }
            };

            var ex = new Exception("message");
            await subscription.OnErrorAsync(1, ex);

            Assert.True(subscription.Failed);
            Assert.Same(ex, trackedException);
            Assert.Equal(1, trackedPosition);
        }

        [Fact]
        public async Task should_call_on_start_delegate()
        {
            long trackedPosition = 0;
            var subscription = new LambdaSubscription(_empty)
            {
                OnStart = position =>
                {
                    trackedPosition = position;
                    return Task.CompletedTask;
                }
            };

            await subscription.OnStartAsync(1);
            Assert.Equal(1, trackedPosition);
        }

        [Fact]
        public async Task should_call_on_stop_delegate()
        {
            long trackedPosition = 0;
            var subscription = new LambdaSubscription(_empty)
            {
                OnStop = position =>
                 {
                     trackedPosition = position;
                     return Task.CompletedTask;
                 }
            };

            await subscription.StoppedAsync(1);
            Assert.Equal(1, trackedPosition);
            Assert.True(subscription.ReadCompleted);
        }

        [Fact]
        public async Task should_call_on_complete_delegate()
        {
            long trackedPosition = 0;
            var subscription = new LambdaSubscription(_empty)
            {
                OnComplete = position =>
                {
                    trackedPosition = position;
                    return Task.CompletedTask;
                }
            };

            await subscription.CompletedAsync(1);

            Assert.Equal(1, trackedPosition);
            Assert.True(subscription.ReadCompleted);
        }
    }
}
