using BlobLockDemo.Helpers;
using BlobLockDemo.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using proMX.proCrastination.Azure.Common;
using System;
using System.Threading.Tasks;
using static proMX.proCrastination.Common.LoggerDelegate;

namespace BlobLockDemo
{
   public static class CoreDemoFunction
   {
      [FunctionName("CoreDemoFunction")]
      public static async Task Run(
         [ServiceBusTrigger(Constants.QueueNames.DemoQueue, AccessRights.Listen, Connection = Constants.Settings.SBConnStr)]
         MxDemoMessage queueMessage,

         [ServiceBus(Constants.QueueNames.DemoQueue, AccessRights.Send, Connection = Constants.Settings.SBConnStr, EntityType = EntityType.Queue)]
         IAsyncCollector<MxDemoMessage> queueCollector,

         TraceWriter log)
      {
         var messageId = queueMessage.Id;
         var logger = IntegrationHelper.GetLogger(log);
         logger($"C# ServiceBus queue trigger function processed message: {messageId}");

         try
         {
            await new LockHelper(
               "demoscope",
               messageId.ToString(),
               logger)
            .RunUnderLockAsync(
               async () =>
               {
                  logger("Starting Core Logic.");
                  await CoreLogic(queueMessage, logger);
                  logger("Core Logic finished.");
               },
               async () =>
               {
                  logger("Retriggering by adding message again into the queue.");
                  await queueCollector.AddAsync(queueMessage);
               }
            );
         }
         catch (Exception ex)
         {
            logger($"{ex}");
            throw;
         }

         logger("Done.");
      }

      static async private Task CoreLogic(MxDemoMessage messageToProcess, DxLogger logger)
      {
         // do something heavy here

         await Task.Delay(1000 * 60);
      }
   }
}
