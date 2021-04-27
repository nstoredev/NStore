using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Tutorial.Support;

namespace NStore.Tutorial.Tutorials
{
    public class T00_Persistence_Append : AbstractTutorial
    {
        protected override async Task RunAsync()
        {
            //
            // Setup
            //
            //
            // Persistence
            //    is a sequence of Chunks
            //    has a global monotonic Position
            //    has partitions with own Index
            //
            var persistence = _runtime.Instrument(new InMemoryPersistence(), "memory-streams");

            //
            // Act
            //
            var chunk = await persistence.AppendAsync
            (
                partitionId: "User_1/Logins",
                payload: "2019-01-22T22:00:12Z",
                
                // optional
                index: 123,
                operationId:"idempotency key"
            );
            
            Logger.LogDebug("Chunk {chunk}", SerializationHelper.ToJson(chunk));
        }
    }
    
    public class T01_Persistence_Append_with_idempotency : AbstractTutorial
    {
        protected override async Task RunAsync()
        {
            //
            // Setup
            //
            //
            // Persistence
            //    is a sequence of Chunks
            //    has a global monotonic Position
            //    has partitions with own Index
            //
            var persistence = _runtime.Instrument(new InMemoryPersistence(), "memory-streams");

            //
            // Act
            //
            var chunk = await persistence.AppendAsync
            (
                partitionId: "User_1/Logins",
                payload: "2019-01-22T22:00:12Z",
                index:1,
                operationId:"There can be only one"
            );
            Logger.LogDebug("Append succeeded {chunk}", SerializationHelper.ToJson(chunk));

            // 
            chunk = await persistence.AppendAsync
            (
                partitionId: "User_1/Logins",
                payload: "2019-01-22T22:10:12Z",
                index:2,
                operationId:"There can be only one"
            );

            if (chunk == null)
            {
                Logger.LogDebug("Append skipped");
            }
        }
    }

}