using System;
using System.IO;
using System.Linq;
using DataFile;
using DataFile.Models.Database.Adapters;
using TestConsole.Properties;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var start = DateTime.Now;
            const string fileName = "16K";
            var fileDirectory = new DirectoryInfo(@"C:\Users\kelvin.delmonte\Desktop\TestDataFiles");
            var targetFile = fileDirectory.GetFiles("*" + fileName + "*").First();
            Console.WriteLine("Processing {0}", targetFile.FullName);
            var transactSqlAdapter = new TransactSqlAdapter(Settings.Default.ConnString, Settings.Default.ImportDirectory)
            {
                CommandTimeout = 0
            };
            using (var dataFile = new DataFileInfo(targetFile.FullName, true, transactSqlAdapter) { TableName = fileName })
            {
                dataFile.BeginDatabaseSession();
            }
            var end = DateTime.Now;
            var timeElapsed = end - start;
            Console.WriteLine("Total time elapsed: {0} hour(s), {1} minute(s), {2} second(s)", timeElapsed.Hours,
                timeElapsed.Minutes, timeElapsed.Seconds);
            Console.Beep();
            Console.ReadKey();
        }
    }
}
