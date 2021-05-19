using System;
using System.Collections.Generic;

namespace proMX.proCrastination.Common
{
   static public class LoggerDelegate
   {
      public delegate void DxLogger(string message);

      static public DxLogger GetNestedLogger(this DxLogger logger, string prefix)
         => (message) => logger($"{prefix} - {message}");

      static public DxLogger GetTimestampedLogger(this DxLogger logger)
      {
         return (message) => logger($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffffff K")}] {message}");
      }

      static public DxLogger GetDeltaTimestampedLogger(this DxLogger logger)
      {
         var start = DateTime.Now;
         return (message) => logger($"[{(DateTime.Now - start)}] {message}");
      }

      static public void LogCollection(this DxLogger logger, string headerMessage, IEnumerable<object> collection)
      {
         logger(headerMessage);

         var index = 0;
         foreach (var item in collection)
            logger($"\t[{index++}] '{item}'");
      }

      static public DxLogger GetCompositeLogger(this DxLogger loggerA, DxLogger loggerB) =>
         (message) =>
         {
            loggerA(message);
            loggerB(message);
         };
   }
}
