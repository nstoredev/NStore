using System;

namespace NStore.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var app = new SampleApp())
            {
                app.CreateRooms();
                Console.ReadLine();

                app.ShowRooms();
                Console.ReadLine();
            }
        }
    }
}