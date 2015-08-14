using System.Collections.Generic;

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
