using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DataFile.Models
{
    public class DataFileLayout
    {
        public DataFileColumnList Columns { get; set; }
        public string Name { get; set; }
        public bool HasColumnHeaders { get; set; }
        public int Width { get; set; }
        public string TextQualifier { get; set; }

        private DataFileFormat _format;
        public DataFileFormat Format
        {
            get { return _format; }
            set
            {
                _format = value;
                var delimiter = DataFileFieldDelimiter.ByFormat(_format);
                if (delimiter != null)
                {
                    _fieldDelimiter = delimiter;
                }
            }
        }

        
        private string _fieldDelimiter;
        public string FieldDelimiter
        {
            get { return _fieldDelimiter; }
            set
            {
                _fieldDelimiter = value;
                var derivedFormat = DataFileFieldDelimiter.GetFormatByDelimiter(_fieldDelimiter);
                if (derivedFormat != null)
                {
                    _format = (DataFileFormat) derivedFormat;
                }
            }
        }

        public DataFileLayout()
        {
            Columns = new DataFileColumnList();
            TextQualifier = "\"";
        }

        public DataFileLayout Clone()
        {
            return Utilities.CloneWithSerialization<DataFileLayout>(this);
        }
    }
}