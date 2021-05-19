using Microsoft.Azure.WebJobs.Host;
using static proMX.proCrastination.Common.LoggerDelegate;

namespace BlobLockDemo.Helpers
{
   static public class IntegrationHelper
   {
      static public DxLogger GetLogger(TraceWriter log) =>
         (message) => log.Info(message);
   }
}
