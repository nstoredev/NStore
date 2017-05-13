using BenchmarkDotNet.Running;

namespace NStore.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<AppendOnlyBenchmark>();
        }
    }
}
