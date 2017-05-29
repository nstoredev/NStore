using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence
{
    public class TaskProfilingInfo
    {
        private long _calls;
        private long _exceptions;
        private long _ticks;
        private long _counter1;

        public string Name { get; }

        public TimeSpan Elapsed => TimeSpan.FromMilliseconds((double)_ticks / TimeSpan.TicksPerMillisecond);
        public long Calls => _calls;
        public long Exceptions => _exceptions;
        public long Counter1 => _counter1;
        public string Counter1Name { get; }

        public TaskProfilingInfo(string name) : this(name, null)
        {
        }

        public TaskProfilingInfo(string name, string counter1Name)
        {
            Name = name;
            Counter1Name = counter1Name ?? "cnt1";
        }

        public async Task CaptureAsync(Func<Task> task)
        {
            Interlocked.Increment(ref _calls);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                await task().ConfigureAwait(false);
            }
            catch (Exception)
            {
                Interlocked.Increment(ref _exceptions);
                throw;
            }
            finally
            {
                sw.Stop();
                Interlocked.Add(ref _ticks, sw.Elapsed.Ticks);
            }
        }

        private string Format(long value)
        {
            return value.ToString().PadLeft(10);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0} {1} calls", Name.PadRight(30), Format(_calls));

            sb.AppendFormat(" took {0}ms.", Format(Elapsed.Milliseconds));

            if (_counter1 > 0)
            {
                sb.AppendFormat(" {0} {1}", Format(_counter1), Counter1Name );
            }

            if (_exceptions > 0)
            {
                sb.AppendFormat(" {0} exceptions", Format(_exceptions));
            }

            return sb.ToString();
        }

        public void IncCounter1()
        {
            Interlocked.Increment(ref _counter1);
        }

        public T Capture<T>(Func<T> func)
        {
            Interlocked.Increment(ref _calls);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                return func();
            }
            catch (Exception)
            {
                Interlocked.Increment(ref _exceptions);
                throw;
            }
            finally
            {
                sw.Stop();
                Interlocked.Add(ref _ticks, sw.Elapsed.Ticks);
            }
        }
    }
}