using System;

namespace NStore.Sample.Support
{
    public class ColoredConsoleReporter : IReporter
    {
        private static readonly object Lock = new object();
        private readonly string _name;
        private readonly ConsoleColor _color;

        public ColoredConsoleReporter(string name, ConsoleColor color)
        {
            _name = name;
            _color = color;
        }

        public void Report(string message)
        {
            lock (Lock)
            {
                var color = Console.ForegroundColor;
                Console.ForegroundColor = _color;
                Console.WriteLine($"{_name.PadRight(20)}: {message}");
                Console.ForegroundColor = color;
            }
        }
    }
}