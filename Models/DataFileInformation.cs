using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models
{
    public class DataFileInformation
    {
        public int TotalRecords;
        public Dictionary<string, int> ColumnLengths { get; set; }

        public DataFileInformation()
        {
            ColumnLengths = new Dictionary<string, int>();
        }

    }
}
