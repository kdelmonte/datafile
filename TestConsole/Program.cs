using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataFile;
using DataFile.Models.Database;
using DataFile.Models.Database.Interfaces;
using TestConsole.Properties;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var start = DateTime.Now;
            const string fileName = "15SS4 OR March 2015 OR 2 of 4 Rep Non-PAVs.xlsx";
            var path = Path.Combine(@"C:\Users\kelvin.delmonte\Desktop\TestDataFiles\",fileName);
            Console.WriteLine("Processing {0}", path);
            var tSqlInterface = new TransactSqlInterface(Settings.Default.ConnString, Settings.Default.ImportDirectory)
            {
                CommandTimeout = 0
            };
            using (var fi = new DataFileInfo(path, true, tSqlInterface))
            {
                var selectQuery = fi.CreateDatabaseCommand().Select(fi.Columns.First()).Limit(20);
                fi.QueryToTable(Settings.Default.ConnString, fi.NameWithoutExtension, selectQuery);
            }
            var end = DateTime.Now;
            var timeElapsed = end - start;
            Console.WriteLine("Total time elapsed: {0} hour(s), {1} minute(s), {2} second(s)", timeElapsed.TotalHours,
                timeElapsed.TotalMinutes, timeElapsed.TotalSeconds);
            Console.Beep();
            Console.ReadKey();
        }
    }
}
