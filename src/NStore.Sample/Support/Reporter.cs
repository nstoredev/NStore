using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Sample.Support
{
    public interface IReporter
    {
        void Report(string message);
    }

    public class ColoredConsoleReporter : IReporter
    {
        private static readonly object Lock = new object();
        private readonly ConsoleColor _color;

        public ColoredConsoleReporter(ConsoleColor color)
        {
            _color = color;
        }

        public void Report(string message)
        {
            lock (Lock)
            {
                var color = Console.ForegroundColor;
                Console.ForegroundColor = _color;
                Console.WriteLine(message);
                Console.ForegroundColor = color;
            }
        }
    }
}
