using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataFile;
using DataFile.Models.Database.Interfaces;
using TestConsole.Properties;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            const string fileName = "15SS4 OR March 2015 OR 2 of 4 Rep Non-PAVs.xlsx";
            var path = Path.Combine(@"C:\Users\kelvin.delmonte\Desktop\TestDataFiles\",fileName);
            var tSqlInterface = new TransactSqlInterface(Settings.Default.ConnString, Settings.Default.ImportDirectory);
            using (var fi = new DataFileInfo(path, true, tSqlInterface))
            {
                fi.QueryToTable(Settings.Default.ConnString, fi.NameWithoutExtension);
            }
        }
    }
}
