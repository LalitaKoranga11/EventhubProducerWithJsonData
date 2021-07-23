using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading.Tasks;

namespace EventhubProducer
{
    class Program
    {
        // connection string to the Event Hubs namespace
        private const string connectionString = "Endpoint=sb://demoeventhubnamespace11.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=HvePeNAAVuxCWDKoo/teQPCwL1dlQslS5IARJvzvnR4=";

        // name of the event hub
        private const string eventHubName = "myeventhub";

        // number of events to be sent to the event hub
        private const int numOfEvents = 5;

        // The Event Hubs client types are safe to cache and use as a singleton for the lifetime
        // of the application, which is best practice when events are being published or read regularly.
        static EventHubProducerClient producerClient;

        static async Task Main()
        {
            // Create a producer client that you can use to send events to an event hub
            producerClient = new EventHubProducerClient(connectionString, eventHubName);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            String messageTrue = "{ \"devicedId\" : \"123\", \"condition\" : true, \"message\" : \"sample\"}";
            String messageFalse = "{ \"devicedId\" : \"123\", \"condition\" : false, \"message\" : \"sample\"}";

            for (int i = 1; i <= numOfEvents; i++)
            {
                string message = messageTrue;
                if (i % 2 != 0) message = messageFalse;

                /*if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }*/

                if (!eventBatch.TryAdd(new EventData(message)))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }

            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine("A batch of " + numOfEvents + " events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
        }
    }
}
