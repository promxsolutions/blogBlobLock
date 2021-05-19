using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Configuration;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using static proMX.proCrastination.Common.LoggerDelegate;

namespace proMX.proCrastination.Azure.Common
{
   /// Redesign:
   /// How should this Lock work? What do we expect?
   /// 1) Lock should be created with two parameters in mind: Scope and Key
   /// for example:
   ///   Scope = "opportunity"
   ///   Key = "40b51782-3e27-419f-aa02-e44e4645e479"
   /// Note: internally 
   ///   Scope will be used as container name 
   ///   while Key will be used as blob name (blob inside container)
   /// 
   /// 2) The content of the blob can be a Timestamp, 
   /// so that we can tell if blob has beed created long time ago
   /// and should be considered as 'expired'.
   /// 
   /// 3) Blob Lease can be acquired for [15 .. 60] seconds or indefinitelly, 
   /// which does not suit us as we anticipate several minutes of processing.
   /// Therefore we should be using Blob Lease Lock internally only 
   /// to protect from parallel work on given blob
   /// 
   /// for usage sample check TestLock.cs

   /// TODO: BlobLock should not know/care 
   /// about content of the blob.
   /// I.e. everything related to TimeStamp should be done 
   /// outside of BlobLock

   public class BlobLock : IDisposable
   {
      /// configuration
      static public string StorageAccountAppSettingName = "AzureWebJobsStorage";
      static private TimeSpan LeaseDuration = TimeSpan.FromSeconds(15);
      static public bool IsLoggingOn { get; set; } = true;

      /// members
      private CloudBlobClient blobClient { get; set; }
      private CloudBlockBlob blob { get; set; }
      private string lease { get; set; }

      private DxLogger logger;
      private bool shouldDeleteBlob = false;

      /// ctor
      public BlobLock(string scope, string key, DxLogger logger)
      {
         this.logger = IsLoggingOn
            ? logger.GetNestedLogger($"BlobLock({scope}/{key})")
            : DevNull();

         this.logger("Ctor starts.");

         ValidateScopeAndKey(scope, key);

         var leaseTask = AquireLeaseAsync(scope, key);
         leaseTask.Wait();

         this.logger("Ctor ends.");
      }
      public void Dispose()
      {
         logger($"Dispose() starts.");

         if (!string.IsNullOrWhiteSpace(lease))
         {
            try
            {
               blob?.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(lease))
                  .Wait();
               logger($"Lease Released.");

               if (shouldDeleteBlob)
               {
                  blob?.DeleteIfExistsAsync()
                     .Wait();
                  logger($"Blob Deleted.");
               }
            }
            catch (AggregateException aggEx)
            {
               logger($"AggregateException...");

               if (!(aggEx.InnerException is StorageException)) // just in case some other process immediately got a lease on it again
                  throw;
            }
            catch (StorageException)
            {
               logger($"StorageException...");

               // just in case some other process immediately got a lease on it again
            }
         }

         logger($"Dispose() ends.");
      }

      public async Task<DateTime?> GetTimestampAsync()
      {
         var content = await blob.DownloadTextAsync();
         if (DateTime.TryParse(content, out var timeStamp))
         {
            logger($"GetTimestampAsync(): '{timeStamp}'.");
            return timeStamp;
         }

         logger($"GetTimestampAsync(): timestamp not found/parsed.");
         return null;
      }
      public async Task SetTimestampAsync()
      {
         var timeStamp = DateTime.UtcNow.ToString();
         logger($"SetTimestampAsync(): '{timeStamp}'.");
         await blob.UploadTextAsync(
            timeStamp,
            Encoding.UTF8,
            AccessCondition.GenerateLeaseCondition(lease),
            new BlobRequestOptions
            {
               RetryPolicy = new NoRetry()
            },
            null);
      }
      public void DeleteBlobOnRelease()
      {
         shouldDeleteBlob = true;
         logger($"DeleteBlobOnRelease(): Blob marked for deletion.");
      }


      static private void ValidateScopeAndKey(string scope, string key)
      {
         if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentNullException("'key' is null");
         if (string.IsNullOrWhiteSpace(scope))
            throw new ArgumentNullException("'scope' is null");
      }
      private async Task AquireLeaseAsync(string scope, string key)
      {
         logger($"AquireLeaseAsync() starts.");

         var container = GetContainer(scope);

         // Try to get an (LeaseDuration) lease on the key object
         while (true)
         {
            logger("Next iteration starts.");

            try
            {
               await AquireLeaseCoreAsync(container, key);

               if (string.IsNullOrWhiteSpace(lease))
               {
                  logger($"Lease is empty.");

                  // we did not get a lease. Let's wait some time between 0.25 and 1 sec and try again, until we succeed
                  await RandomizedDelayAsync();
               }
               else
               {
                  // YAY! We got it.
                  // at this time both blob and lease are initialized

                  logger($"Lease Aquired: '{lease}'.");
                  return;
               }
            }
            catch (StorageException ex)
            {
               logger($"StorageException ({ex.RequestInformation.HttpStatusCode}).");

               if (ex.RequestInformation.HttpStatusCode != 409 && // 409 is for some reason perfectly valid here...
                  ex.RequestInformation.HttpStatusCode != 404 && // 404 probably interference with another thread
                  ex.RequestInformation.HttpStatusCode != 412) // 412 -> There is already a lease
                  throw;

               await RandomizedDelayAsync();
            }
            catch (WebException wEx)
            {
               logger($"WebException...");

               if ((wEx.Response as HttpWebResponse)?.StatusCode != HttpStatusCode.NotFound)
                  throw;

               // probably deleted by dispose?
               await RandomizedDelayAsync();
            }
            catch (Exception ex)
            {
               logger($"Exception...");
               throw;
            }
         }
      }
      private async Task AquireLeaseCoreAsync(CloudBlobContainer container, string key)
      {
         var requestOptions = new BlobRequestOptions
         {
            RetryPolicy = new NoRetry()
         };

         await container.CreateIfNotExistsAsync(requestOptions, null);

         blob = container.GetBlockBlobReference(key);

         /// it's possible that blob does not exist yet
         /// well, most of the time it does not exist
         if (false == blob.Exists())
         {
            logger("Blob does not exists yet, creating (content == empty string).");
            //await SetTimestampAsync();
            await blob.UploadTextAsync("");

            /// up till here two parallel processes will compete for blob
            /// even if both processes initiate blob creation - only one will acquire lease in next line
         }

         lease = await blob.AcquireLeaseAsync(LeaseDuration);
      }
      private CloudBlobContainer GetContainer(string scope)
      {
         /// OP: TODO: Storage Account connection string should be coming from outside!
         var storageConnectionString = ConfigurationManager.AppSettings[StorageAccountAppSettingName];
         var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
         blobClient = storageAccount.CreateCloudBlobClient();
         var container = blobClient.GetContainerReference(scope);
         return container;
      }
      private async Task RandomizedDelayAsync()
      {
         var msToSleep =
            new Random(Guid.NewGuid().GetHashCode())
            .Next(250, 1000);

         logger($"Going to sleep for {msToSleep} ms.");

         await Task.Delay(
            TimeSpan.FromMilliseconds(msToSleep));
      }

      static private DxLogger DevNull() => (message) => { };
   }
}
