using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Streams;

namespace NStore.Core.Processing
{
    public static class StreamProcessorExtensions
    {
        public static StreamProcessor Aggregate(this IReadOnlyStream stream)
        {
            var processor = new StreamProcessor(stream);
            return processor;
        }

        [Obsolete("Use Aggregate")]
        private static StreamProcessor Fold(this IReadOnlyStream stream)
        {
            return stream.Aggregate();
        }

        public static Task<TResult> AggregateAsync<TResult>(this IReadOnlyStream stream) where TResult : new()
        {
            return stream.Aggregate().RunAsync<TResult>();
        }

        public static Task<TResult> AggregateAsync<TResult>(
            this IReadOnlyStream stream, 
            Func<TResult, object, object> processor
        ) where TResult : new()
        {
            return stream.Aggregate().RunAsync<TResult>(
                new DelegateToLambdaPayloadProcessor<TResult>(processor),
                default(CancellationToken)
            );
        }

        public static Task<TResult> AggregateAsync<TResult>(
            this IReadOnlyStream stream, 
            Func<TResult, object, Task<object>> processor
        ) where TResult : new()
        {
            return stream.Aggregate().RunAsync<TResult>(
                new DelegateToLambdaPayloadProcessor<TResult>(processor),
                default(CancellationToken)
            );
        }

        public static Task<TResult> AggregateAsync<TResult>(
            this IReadOnlyStream stream, 
            IPayloadProcessor processor,
            CancellationToken cancellationToken
            ) where TResult : new()
        {
            return stream.Aggregate().RunAsync<TResult>(processor, cancellationToken);
        }
    }
}