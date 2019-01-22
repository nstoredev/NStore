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
            await new T10_Aggregate_Create_Read_Update().ShowAsync();
            await new T11_Aggregate_Delete().ShowAsync();
        }
    }
}