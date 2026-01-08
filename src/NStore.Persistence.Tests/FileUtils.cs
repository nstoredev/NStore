using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

#nullable enable

namespace NStore.Persistence.Tests
{
    public static class FileUtils
    {
        public static void SafeDelete(string path, TimeSpan? timeout = null)
        {
            if (string.IsNullOrEmpty(path) || !File.Exists(path))
            {
                return;
            }

            var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(10);
            var retryDelay = TimeSpan.FromSeconds(1);
            var sw = Stopwatch.StartNew();
            Exception? lastException = null;

            while (sw.Elapsed < effectiveTimeout)
            {
                try
                {
                    if (!File.Exists(path))
                    {
                        return;
                    }

                    File.Delete(path);
                    return;
                }
                catch (IOException ex)
                {
                    lastException = ex;
                    Thread.Sleep(retryDelay);
                }
                catch (UnauthorizedAccessException ex)
                {
                    lastException = ex;
                    Thread.Sleep(retryDelay);
                }
            }

            if (File.Exists(path))
            {
                var message = $"Could not delete file '{path}' within {effectiveTimeout.TotalSeconds} seconds.";
                if (lastException != null)
                {
                    throw new IOException(message, lastException);
                }

                throw new IOException(message);
            }
        }
    }
}
