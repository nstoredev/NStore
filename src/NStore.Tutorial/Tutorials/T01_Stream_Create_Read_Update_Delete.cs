using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NStore.Core.Streams;
using NStore.Tutorial.Support;

namespace NStore.Tutorial.Tutorials
{
    // ReSharper disable once InconsistentNaming
    public class T01_Stream_Create_Read_Update_Delete : AbstractTutorial
    {
        public class Payload
        {
            public int UserId { get; set; }
            public string EventName { get; set; }
            public DateTime Timestamp { get; set; }
        }

        protected override async Task RunAsync()
        {
            //
            // Setup
            //
            var payload = new Payload()
            {
                Timestamp = DateTime.UtcNow,
                UserId = 1,
                EventName = "logged_in"
            };

            var stream = OpenStream("stream_1");


            // 
            // ACT
            //
            var chunk = await stream.AppendAsync(payload);

            Logger.LogDebug("Payload persisted {dump}", SerializationHelper.ToJson(chunk));
        }
    }
}