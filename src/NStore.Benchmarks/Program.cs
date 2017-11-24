using BenchmarkDotNet.Running;

namespace NStore.Benchmarks
{
    static class Program
    {
        public static void Main(string[] args)
        {
            //BenchmarkRunner.Run<MongoBatchWriteBenchmark>();
            //BenchmarkRunner.Run<MongoTplAppendAsyncBenchmark>();
            BenchmarkRunner.Run<SqliteBatchWriteBenchmark>();
        }
    }
}
