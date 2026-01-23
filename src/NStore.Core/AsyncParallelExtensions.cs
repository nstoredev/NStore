using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core
{
    public static class AsyncParallelExtensions
    {
        public static Task ForEachAsync<T>(
            this IEnumerable<T> source,
            int maxDegreeOfParallelism,
            Func<T, CancellationToken, Task> body,
            CancellationToken cancellationToken = default)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (body == null) throw new ArgumentNullException(nameof(body));
            if (maxDegreeOfParallelism <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));

            return ForEachAsyncCore(source, maxDegreeOfParallelism, body, cancellationToken);
        }

        private static async Task ForEachAsyncCore<T>(
            IEnumerable<T> source,
            int maxDegreeOfParallelism,
            Func<T, CancellationToken, Task> body,
            CancellationToken cancellationToken)
        {
            using var throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var item in source)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await body(item, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        throttler.Release();
                    }
                }, cancellationToken));
            }

            List<Exception> exceptions = null;

            while (tasks.Count > 0)
            {
                var completed = await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks.Remove(completed);

                try
                {
                    await completed.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    exceptions ??= new List<Exception>();
                    exceptions.Add(ex);
                }
            }

            if (exceptions != null)
            {
                throw new AggregateException(exceptions);
            }
        }
    }
}
