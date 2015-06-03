using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataFile;
using DataFile.DatabaseInterfaces;
using TestConsole.Properties;

namespace TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            const string path = @"C:\Users\kelvin.delmonte\Desktop\TEST.csv";
            var tSqlInterface = new TransactSqlInterface(Settings.Default.ConnString, Settings.Default.ImportDirectory);
            using (var fi = new DataFileInfo(path, true, tSqlInterface))
            {
                fi.ImportIntoTable(Settings.Default.ConnString, "WHAT");
            }
            
        }
    }
}
