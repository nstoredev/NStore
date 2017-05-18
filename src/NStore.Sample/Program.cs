using System;

namespace NStore.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var app = new SampleApp(32))
            {
                Console.WriteLine("Press ENTER to start and wait projections, then press ENTER again to show data & stats.");
                Console.ReadLine();
                app.CreateRooms();
                app.AddSomeBookings(64);

                Console.ReadLine();

                app.ShowRooms();
                app.DumpMetrics();

                Console.WriteLine("Press ENTER to close.");
                Console.ReadLine();
            }
        }
    }
}