using System.Collections.Generic;
using System.Linq;

namespace DataFile.Models
{
    public class DataFileValueError
    {
        public bool Required { get; set; }
        public bool Pattern { get; set; }
        public bool MinLength { get; set; }
        public bool MaxLength { get; set; }
        public bool AllowedValues { get; set; }
        public bool DataType { get; set; }
    }
}