using System;

namespace NStore.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            var engine = new Engine();
            engine.CreateRooms().Wait();


            Console.WriteLine("Press any key to exit");
            Console.ReadKey();

            engine.ShowRooms();
        }
    }
}