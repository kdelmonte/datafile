using System.Collections.Generic;
using System.Linq;

namespace DataFile.Models
{
    public class DataFileValueValidity
    {
        public int RowNumber { get; set; }
        public int ColumnIndex { get; set; }
        public DataFileValueError Error { get; set; }

        public bool Valid => Error == null || (!Error.Required && !Error.Pattern && !Error.MinLength && !Error.MaxLength && !Error.AllowedValues && !Error.DataType);

        public bool Invalid => !Valid;

        public DataFileValueValidity()
        {
            Error = new DataFileValueError();
        }
    }
}