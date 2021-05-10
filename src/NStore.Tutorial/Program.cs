using System.Threading.Tasks;
using NStore.Tutorial.Tutorials;

namespace NStore.Tutorial
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // TutorialRuntime.Initializer = () => TutorialRuntime.UseSqlServer(
            //     "Server=localhost;Database=NStore;User Id=sa;Password=MsSqlDock3r;"
            // );   
            
            TutorialRuntime.Initializer = TutorialRuntime.UseInMemory;

            // Persistence
            await new T00_Persistence_Append().ShowAsync();
            await new T01_Persistence_Append_with_idempotency().ShowAsync();

            // Streams
            await new T01_Stream_Create_Read_Update_Delete().ShowAsync();

            // Aggregates
            await new T10_Aggregate_Create_Read_Update().ShowAsync();
            await new T11_Aggregate_Delete().ShowAsync();
        }
    }
}