using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Sample.Support
{
    public interface IReporter
    {
        void Report(string kind, string message);
    }

    public class ColoredConsoleReporter : IReporter
    {
        private static readonly object Lock = new object();

        public void Report(string kind, string message)
        {
            var newcolor = GetColor(kind);
            lock (Lock)
            {
                var color = Console.ForegroundColor;
                Console.ForegroundColor = newcolor;
                Console.WriteLine(message);
                Console.ForegroundColor = color;
            }
        }

        private ConsoleColor GetColor(string kind)
        {
            switch (kind)
            {
                case "engine": return ConsoleColor.Green;
                case "prjengine": return ConsoleColor.Yellow;
                case "RoomsOnSaleProjection": return ConsoleColor.DarkGray;
                case "ConfirmedBookingsProjection": return ConsoleColor.DarkCyan;
            }

            return ConsoleColor.White;
        }
    }
}
