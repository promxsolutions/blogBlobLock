using BlobLockDemo.Helpers;
using BlobLockDemo.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace BlobLockDemo
{
   public static class StartDemoFunction
   {
      [FunctionName("StartDemoFunction")]
      public static async Task<HttpResponseMessage> Run(
         [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] 
         HttpRequestMessage request,

         [ServiceBus(Constants.QueueNames.DemoQueue, AccessRights.Send, Connection = Constants.Settings.SBConnStr, EntityType = EntityType.Queue)]
         IAsyncCollector<MxDemoMessage> queueCollector,

         TraceWriter log)
      {
         log.Info("StartDemoFunction was triggered...");

         try
         {
            // Check request
            var payload = await request.Content.ReadAsAsync<MxDemoMessage>();

            // we can validate payload here


            // Put the message to the queue, to start the actual work
            await queueCollector.AddAsync(payload);

            return request.CreateResponse(HttpStatusCode.OK);
         }
         catch (Exception ex)
         {
            log.Error(ex.ToString());
            throw;
         }
      }
   }
}
