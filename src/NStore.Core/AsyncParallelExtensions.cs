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
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();
            Exception firstException = null;
            OperationCanceledException cancellation = null;

            try
            {
                foreach (var item in source)
                {
                    await throttler.WaitAsync(cts.Token).ConfigureAwait(false);

                    // If a previous task has failed, stop scheduling new work
                    if (Volatile.Read(ref firstException) != null)
                    {
                        break;
                    }

                    tasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            await body(item, cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (!(ex is OperationCanceledException))
                        {
                            // Record the first failure and cancel remaining work
                            Interlocked.CompareExchange(ref firstException, ex, null);
                            cts.Cancel();
                            throw;
                        }
                        finally
                        {
                            throttler.Release();
                        }
                    }));
                }
            }
            catch (OperationCanceledException ex)
            {
                cancellation = ex;
            }

            // Wait for all already-running tasks to complete
            foreach (var task in tasks)
            {
                try
                {
                    await task.ConfigureAwait(false);
                }
                catch (OperationCanceledException ex)
                {
                    cancellation ??= ex;
                }
                catch
                {
                    // already captured via Interlocked.CompareExchange
                }
            }

            throttler.Dispose();

            // Task failure takes priority over cancellation
            if (firstException != null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstException).Throw();
            }

            if (cancellation != null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(cancellation).Throw();
            }
        }
    }
}
