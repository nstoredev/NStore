using System;
using System.Threading.Tasks;
using NStore.Core.Streams;
using NStore.Domain;

namespace NStore.Tutorial.Tutorials
{
    /// <summary>
    /// Plumbing stuff
    /// </summary>
    public abstract class AbstractTutorial
    {
        private readonly TutorialRuntime _runtime;
        protected AbstractTutorial()
        {
            _runtime = TutorialRuntime.CreateDefaultRuntime();
        }

        protected IRepository CreateRepository()
        {
            return _runtime.CreateRepository();
        }

        protected IStream OpenStream(string id)
        {
            return _runtime.OpenStream(id);
        }
        
        protected abstract Task RunAsync();

        public async Task ShowAsync()
        {
            Console.Clear();
            
            _runtime.Log($"\nRunning {this.GetType().Name}\n");
            await RunAsync();
            _runtime.Shutdown();
            _runtime.Log("\nPress ENTER to continue\n");

            Console.ReadLine();
        }
    }
}