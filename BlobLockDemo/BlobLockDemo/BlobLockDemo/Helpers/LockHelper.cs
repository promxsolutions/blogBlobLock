using System;
using System.Threading.Tasks;
using static proMX.proCrastination.Common.LoggerDelegate;

namespace proMX.proCrastination.Azure.Common
{
   public class LockHelper
   {
      private const int CTooOldInMinutes = 15;

      private string scope;
      private string key;
      private DxLogger logger;

      public LockHelper(string scope, string key, DxLogger logger)
      {
         this.scope = scope;
         this.key = key;
         this.logger = logger;
      }

      /// <summary>
      /// Is there another process already running?
      ///   No:   execute Core Logic.
      ///   Yes:  set flag to notify already running process
      ///         that re-trigger is required.
      /// After Core Logic is executed - check if flag is set.
      ///   No:   do nothing
      ///   Yes:  re-trigger
      ///   Both cases:
      ///         cleanup lock and flag blobs.
      /// </summary>
      public async Task RunUnderLockAsync(Func<Task> coreLogic, Func<Task> reTriggerCallback)
      {
         logger("RunUnderLockAsync() starts.");

         logger("Check if Core Logic can run.");
         var canRunCoreLogic = await CheckIfCanRunCoreLogic(scope, key, logger, reTriggerCallback);
         if (canRunCoreLogic)
         {
            logger("Core Logic starts.");
            await coreLogic();
            logger("Core Logic ends.");

            logger("Delete Lock Blob and re-Trigger if Flag was set.");

            var isReTriggerRequired = false;
            using (var blobLock = new BlobLock(scope, GetLockKey(key), logger))
            {
               isReTriggerRequired = await IsFlagSet(scope, key, logger);

               /// even if flag blob did exist before - it does now,
               /// as it was created in IsFlagSet
               /// therefore we need to delete it
               //if (isReTriggerRequired)
               ResetFlag(scope, key, logger);

               ///   and mark blob for delete
               blobLock.DeleteBlobOnRelease();
            }

            if (isReTriggerRequired)
            {
               logger("re-Trigger was requested, re-triggering.");
               await reTriggerCallback();
            }
         }

         logger("RunUnderLockAsync() ends.");
      }


      static private string GetLockKey(string key) => $"{key}-lock";
      static private string GetFlagKey(string key) => $"{key}-flag";

      private async Task<bool> CheckIfCanRunCoreLogic(string scope, string key, DxLogger logger, Func<Task> reTriggerCallback)
      {
         var canRunCoreLogic = false;

         ///   acquire a lock
         using (var blobLock = new BlobLock(scope, GetLockKey(key), logger))
         {
            ///   read timestamp
            var timestamp = await blobLock.GetTimestampAsync();
            if (false == timestamp.HasValue)
            {
               logger("lease is ours, timestamp is not yet set...");
               logger("i.e. this is our blob!");

               ///   initialize blob with timestamp
               await blobLock.SetTimestampAsync();
               canRunCoreLogic = true;
            }
            else
            {
               logger("lease is ours but timestamp is already set...");
               logger("checking if blob can be considered 'expired'.");

               ///   check if blob can be considered 'expired'
               var isLockTooOld = (DateTime.UtcNow - timestamp.Value).TotalMinutes > CTooOldInMinutes;
               if (isLockTooOld)
               {
                  logger("blob is indeed too old.");

                  ///   mark blob for delete
                  blobLock.DeleteBlobOnRelease();

                  ///   re-trigger original event, so new process comes and finds blob/lock non existing
                  await reTriggerCallback();
               }
               else
               {
                  logger("lease is ours, lock is not expired -> set flag and exit");
                  await SetFlag(scope, key, logger);
               }
            }
         }

         return canRunCoreLogic;
      }
      private async Task SetFlag(string scope, string key, DxLogger logger)
      {
         using (var blobLock = new BlobLock(scope, GetFlagKey(key), logger))
         {
            await blobLock.SetTimestampAsync();
         }

         logger("Flag Set.");
      }
      private async Task<bool> IsFlagSet(string scope, string key, DxLogger logger)
      {
         var isFlagSet = false;
         using (var blobLock = new BlobLock(scope, GetFlagKey(key), logger))
         {
            var timestamp = await blobLock.GetTimestampAsync();
            isFlagSet = timestamp.HasValue;
         }

         logger($"IsFlagSet: '{isFlagSet}'.");
         return isFlagSet;
      }
      private void ResetFlag(string scope, string key, DxLogger logger)
      {
         using (var blobLock = new BlobLock(scope, GetFlagKey(key), logger))
         {
            blobLock.DeleteBlobOnRelease();
         }

         logger("Flag Reset.");
      }
   }
}
