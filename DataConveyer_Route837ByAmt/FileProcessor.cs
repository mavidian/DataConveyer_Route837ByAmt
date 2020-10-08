// Copyright © 2019-2020 Mavidian Technologies Limited Liability Company. All Rights Reserved.

using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataConveyer_Route837ByAmt
{
   /// <summary>
   /// Represents Data Conveyer functionality specific to routing X12 837 transactions to one
   /// of the two output files depending on the CLM02 element ("claim monetary amount").
   /// </summary>
   internal class FileProcessor
   {
      private readonly IOrchestrator Orchestrator;

      internal FileProcessor(string inFile, string outLocation)
      {
         var outFileBody = outLocation + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(inFile);
         var outFileExt = Path.GetExtension(inFile);
         var config = new OrchestratorConfig()
         {
            GlobalCacheElements = new string[] { "LowCnt|0", "HighCnt|0", "AllCnt|0", "IsaElems", "GsElems" },
            DefaultX12SegmentDelimiter = "~\r\n",
            InputDataKind = KindOfTextData.X12,
            InputFileName = inFile,
            ClusterMarker = SegmentStartsCluster,
            MarkerStartsCluster = true,  //predicate (marker) matches the first record in cluster
            PrependHeadCluster = true,   // to contain ISA/GS segments for _high file
            AppendFootCluster = true,    // to contain IEA/GE segments for _high file
            RecordInitiator = StoreIsaAndGsSegments,
            PropertyBinEntities = PropertyBinAttachedTo.Clusters,
            DeferTransformation = DeferTransformation.UntilRecordInitiation,
            ConcurrencyLevel = 4,
            TransformerType = TransformerType.Clusterbound,
            ClusterboundTransformer = ProcessX12Transaction,
            RouterType = RouterType.PerCluster,
            ClusterRouter = SendToLowOrHigh,
            OutputDataKind = KindOfTextData.X12,
            OutputFileNames = outFileBody + "_low" + outFileExt + "|" + outFileBody + "_high" + outFileExt  //1st: less than $1,000; 2nd: $1,000 or more
         };

         Orchestrator = OrchestratorCreator.GetEtlOrchestrator(config);
      }

      /// <summary>
      /// Execute Data Conveyer process.
      /// </summary>
      /// <returns>Task containing the process results.</returns>
      internal async Task<ProcessResult> ProcessFileAsync()
      {
         var result = await Orchestrator.ExecuteAsync();
         Orchestrator.Dispose();

         return result;
      }


      /// <summary>
      /// Cluster marker function to bundle all segments of each X12 transaction into a cluster
      /// </summary>
      /// <param name="rec"></param>
      /// <param name="prevRec"></param>
      /// <param name="recNo"></param>
      /// <returns></returns>
      private bool SegmentStartsCluster(IRecord rec, IRecord prevRec, int recNo)
      {
         return new string[] { "ISA", "GS", "ST", "GE", "IEA" }.Contains(rec["Segment"]);
      }  //each transaction is own cluster (plus separate clusters containing envelope segments)


      /// <summary>
      /// Record initiator to store ISA and GS contents in global cache and allow transformation to start upon completing GS segment
      /// </summary>
      /// <param name="rec"></param>
      /// <param name="traceBin"></param>
      /// <returns></returns>
      private bool StoreIsaAndGsSegments(IRecord rec, IDictionary<string, object> traceBin)
      {  //cache ISA and GS contents
         var seg = (string)rec["Segment"];
         //ISA/GS segments cannot be stored in trace bin, as they're are needed at head cluster during transformation.
         if (seg == "ISA") rec.GlobalCache.ReplaceValue<string, string[]>("IsaElems", s => rec.Items.Select(i => i.StringValue).ToArray());
         if (seg == "GS")
         {
            rec.GlobalCache.ReplaceValue<string, string[]>("GsElems", s => rec.Items.Select(i => i.StringValue).ToArray());
            return true; //transformation can start
         }
         if (seg == "ST") rec.GlobalCache.IncrementValue("AllCnt");
         return false;
      }


      /// <summary>
      /// Clusterbound transformer to process X12 transactions:
      /// - define interchange envelopes for the _high file (_low file uses envelopes from input file),
      /// - determine if transaction qualifies for _low or _high file
      /// - update _low and _high counts in global cache
      /// - place the intended target number(1=_low or 2=_high) in the property bin
      /// </summary>
      /// <param name="clstr">X12 transaction to be processed.</param>
      /// <returns>In general, the same X12 transaction as received (but decorated with target in property bin); also, if head/foot cluster, then interchange envelowpe elements.</returns>
      private ICluster ProcessX12Transaction(ICluster clstr)
      {
         if (clstr.StartRecNo == 0) //head cluster
         {  //create ISA & GS to go with _high
            AddSegment(clstr, (string[])clstr.GlobalCache["IsaElems"]);
            AddSegment(clstr, (string[])clstr.GlobalCache["GsElems"]);
         }
         else if (clstr.StartRecNo == -1) //foot cluster
         {  //create IEA & GE to go with _high
            clstr.GlobalCache.AwaitCondition(gc => (int)gc["AllCnt"] == (int)gc["LowCnt"] + (int)gc["HighCnt"]);  //wait until all "regular" clusters are processed
            AddSegment(clstr, new string[] { "GE", clstr.GlobalCache["HighCnt"].ToString(), ((string[])clstr.GlobalCache["GsElems"])[6] });
            AddSegment(clstr, new string[] { "IEA", "1", ((string[])clstr.GlobalCache["IsaElems"])[13].Trim() });
         }
         else  //"regular" clusters
         {
            var chargesAreLow = ChargesAreLow(clstr);  //evaluate if _low or _high
            var seg = (string)clstr[0]["Segment"];
            if (seg == "ST")  //cluster representing claim
            {  //update respective counter
               if (chargesAreLow) clstr.GlobalCache.IncrementValue("LowCnt"); else clstr.GlobalCache.IncrementValue("HighCnt");
            }
            if (seg == "GE")  //cluster with GE segment (note that IEA segment needs no updates)
            {  //update transaction count (_low count)
               clstr.GlobalCache.AwaitCondition(gc => (int)gc["AllCnt"] == (int)gc["LowCnt"] + (int)gc["HighCnt"]);  //wait until all "regular" clusters are processed
               clstr[0][1] = clstr.GlobalCache["LowCnt"].ToString();
            }
            clstr.PropertyBin.Add("TargetNo", chargesAreLow ? 1 : 2);
         }

         return clstr;
      }


      /// <summary>
      /// Cluster router to send an X12 segment to one of the 2 outputs based on data held in the property bin
      /// </summary>
      /// <param name="clstr"></param>
      /// <returns>1 for _low, 2 for _high</returns>
      private int SendToLowOrHigh(ICluster clstr)
      {
         if (clstr.StartRecNo <= 0) return 2;  //head & foot clusters go with _high
         return (int)clstr.PropertyBin["TargetNo"];
      }


      private bool ChargesAreLow(ICluster clstr)
      {  //helper method to determine if claim goes with _low or _high
         //this is determined based on CLM02 element ("claim monetary amount")
         var clm02 = (string)clstr.Records.FirstOrDefault(r => (string)r["Segment"] == "CLM")?[2];
         if (clm02 == null) return true;  //ISA, GS, GE & IEA go with _low
         return float.Parse(clm02) < 1000f; //less than $1,000 means _low
      }


      private void AddSegment(ICluster clstr, string[] elems)
      {  //helper method to add a record representing segment to current cluster
         clstr.AddRecord(new Segment(clstr, elems).ToRecord());
      }


      private class Segment
      {  //helper class representing a single segment of X12 transaction
         private readonly IRecord _segment;
         public Segment(ICluster clstr, string[] elems)
         {
            var templ = clstr.Records.Any() ? clstr[0] : clstr.ObtainEmptyRecord();
            _segment = templ.CreateEmptyX12Segment(elems[0], elems.Length - 1);
            for (int i = 1; i < elems.Length; i++) _segment[i] = elems[i];
         }
         public IRecord ToRecord() { return _segment; }
      }

   }
}
