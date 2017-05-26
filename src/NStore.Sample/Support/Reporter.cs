using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Sample.Support
{
    public interface IReporter
    {
        void Report(string message);
    }

    public class NullReporter : IReporter
    {
        public static readonly IReporter Instance = new NullReporter();

        private NullReporter()
        {
            
        }

        public void Report(string message)
        {
            // nothing to do here...
        }
    }

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
