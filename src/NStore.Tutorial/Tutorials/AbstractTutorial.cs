using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NStore.Core.Persistence;
using NStore.Core.Streams;
using NStore.Domain;

namespace NStore.Tutorial.Tutorials
{
    /// <summary>
    /// Plumbing stuff
    /// </summary>
    public abstract class AbstractTutorial
    {
        protected readonly TutorialRuntime _runtime;
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

        protected ILogger Logger => _runtime.Logger;
    
        protected abstract Task RunAsync();

        public async Task ShowAsync()
        {
            Console.Clear();
            
            Logger.LogInformation($"Running {this.GetType().Name}");
            await RunAsync();
            _runtime.Shutdown();
            Logger.LogInformation("Press ENTER to continue");

            Console.ReadLine();
        }
    }
}