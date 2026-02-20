#if NET6_0_OR_GREATER
// this is an utility only for older version of .NET
#else
using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
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
                    }, cts.Token));
                }
            }
            catch (OperationCanceledException ex)
            {
                cancellation = ex;
            }

            // Wait for all already-running tasks to complete
            cancellation = await AwaitAllTasksAsync(tasks, cancellation).ConfigureAwait(false);

            throttler.Dispose();

            ThrowIfFailed(firstException, cancellation);
        }

        private static async Task<OperationCanceledException> AwaitAllTasksAsync(
            List<Task> tasks, OperationCanceledException cancellation)
        {
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

            return cancellation;
        }

        private static void ThrowIfFailed(Exception firstException, OperationCanceledException cancellation)
        {
            // Match Parallel.ForEachAsync behavior: surface the real failure,
            // not the cancellation it triggered in sibling tasks.
            if (firstException != null)
            {
                ExceptionDispatchInfo.Capture(firstException).Throw();
            }

            if (cancellation != null)
            {
                ExceptionDispatchInfo.Capture(cancellation).Throw();
            }
        }
    }
}
#endif