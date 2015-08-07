using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models
{
    public class DataFileSaveSettings
    {
        public string Path { get; set; }
        public bool Overwrite { get; set; }
        public DataFileLayout Layout { get; set; }
        public List<DataFileColumnMapping> Mappings { get; set; }
        public DataFileSaveSettings()
        {
            Layout = new DataFileLayout();
            Mappings = new List<DataFileColumnMapping>();
        }
        
    }
}
