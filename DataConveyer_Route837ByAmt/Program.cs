// Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Diagnostics;
using System.IO;

namespace DataConveyer_Route837ByAmt
{
   class Program
   {
      // Locations of Data Conveyer input and output:
      private const string InputFolder = @"..\..\..\Data\In";
      private const string OutputFolder = @"..\..\..\Data\Out";

      private static string FullOutputLocation;  // full means absolute path

      static void Main()
      {
         var asmName = System.Reflection.Assembly.GetExecutingAssembly().GetName();
         var fullInputLocation = Path.GetFullPath(InputFolder);
         FullOutputLocation = Path.GetFullPath(OutputFolder);
         Console.WriteLine($"{asmName.Name} v{asmName.Version} started execution on {DateTime.Now:MM-dd-yyyy a\\t hh:mm:ss tt}");
         Console.WriteLine($"DataConveyer library used: {ProductInfo.CurrentInfo.ToString()}");
         Console.WriteLine();
         Console.WriteLine("This application reads an X12 file containing 837 transactions.");
         Console.WriteLine("Each contained transaction is evaluated and routed to 1 of 2 output files (depending on its amount).");
         Console.WriteLine();
         Console.WriteLine($"Input location : {fullInputLocation}");
         Console.WriteLine($"Output location: {FullOutputLocation}");
         Console.WriteLine();

         var dropOffWatcher = new FileSystemWatcher
         {
            Path = InputFolder,
            EnableRaisingEvents = true
         };
         dropOffWatcher.Created += FileDroppedHandler;

         Console.WriteLine($"Waiting for file(s) to be placed at input location...");
         Console.WriteLine("To exit, hit Enter key...");
         Console.WriteLine();

         Console.ReadLine();
      }


      private static async void FileDroppedHandler(object sender, FileSystemEventArgs e)
      {
         // "Fire & forget" async void method is OK here - it is of FileSystemEventHandler delegate type.
         var fname = e.Name;
         Console.WriteLine($"Detected {fname} file... processing started...'.");

         var processor = new FileProcessor(e.FullPath, FullOutputLocation);

         var stopWatch = new Stopwatch();
         stopWatch.Start();
         var result = await processor.ProcessFileAsync();
         stopWatch.Stop();

         if (result.CompletionStatus == CompletionStatus.IntakeDepleted)
         {
            Console.WriteLine($"Processing {fname} file completed in {stopWatch.Elapsed.TotalSeconds.ToString("##0.000")}s.");
            Console.WriteLine($"There have been {result.ClustersRead - 6} claims routed ({result.GlobalCache["LowCnt"]} claims contained less than $1,000).");  //-6 excludes envelopes & head/foot);
         }
         else Console.WriteLine($"Oops! Processing resulted in unexpected status of " + result.CompletionStatus.ToString());
      }

   }
}
