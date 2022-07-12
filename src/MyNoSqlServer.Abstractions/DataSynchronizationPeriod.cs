using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Abstractions
{
    public enum DataSynchronizationPeriod
    {
        Immediately, Sec1, Sec5, Sec15, Sec30, Min1, Asap, Default
    }


    public static class DataSynchronizationPeriodExtensions
    {

        private static readonly Dictionary<DataSynchronizationPeriod, string> PeriodAsString =
            new Dictionary<DataSynchronizationPeriod, string>
            {
                [DataSynchronizationPeriod.Immediately] = "Immediately",
                [DataSynchronizationPeriod.Sec1] = "Sec1",
                [DataSynchronizationPeriod.Sec5] = "Sec5",
                [DataSynchronizationPeriod.Sec15] = "Sec15",
                [DataSynchronizationPeriod.Sec30] = "Sec30",
                [DataSynchronizationPeriod.Min1] = "Min1",
                [DataSynchronizationPeriod.Asap] = "Asap",
                [DataSynchronizationPeriod.Default] = "Default",
            };

        
        private static readonly Dictionary<string, DataSynchronizationPeriod> PeriodAsEnum 
            = new Dictionary<string, DataSynchronizationPeriod>();

        static DataSynchronizationPeriodExtensions()
        {
            foreach (var kvp in PeriodAsString)
            {
                PeriodAsEnum.Add(kvp.Value, kvp.Key);
            }
        }
        
        
        
        public static string AsString(this DataSynchronizationPeriod src, string @default)
        {
            if (@default != null) 
                return PeriodAsString.ContainsKey(src) ? PeriodAsString[src] : @default;
            
            if (PeriodAsString.ContainsKey(src))
                return PeriodAsString[src];
                
            throw new Exception("Invalid Type: "+src);
        }

        public static DataSynchronizationPeriod ParseDataSynchronizationPeriod(this string src, DataSynchronizationPeriod @default)
        {
            if (string.IsNullOrEmpty(src))
                return @default;

            return PeriodAsEnum.ContainsKey(src) ? PeriodAsEnum[src] : @default;
            
        }
    }
}