using NStore.Tutorial.CartDomain;
using System;
using System.Threading.Tasks;
using NStore.Tutorial.Tutorials;

namespace NStore.Tutorial
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await new T01_Create_Read_Update().ShowAsync();
        }
    }
}