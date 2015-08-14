using System.Collections.Generic;

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
