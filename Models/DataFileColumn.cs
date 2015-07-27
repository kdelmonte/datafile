using System;
using System.ComponentModel;

namespace DataFile.Models
{
    public class DataFileColumn
    {
        public string Alias { get; set; }
        public int End { get; set; }
        private object _exampleValue;

        public object ExampleValue
        {
            get
            {
                return _exampleValue;
            }
            set { _exampleValue = ConvertValue(value); }
        }

        public bool FixedWidthMode { get; set; }
        public int Index { get; set; }
        public int Start { get; set; }
        private int _length = -1;
        public string Pattern { get; set; }
        public int? MaxLength { get; set; }

        private Type _dataType;
        public Type DataType
        {
            get { return _dataType; }
            set
            {
                _dataType = value;
                _exampleValue = ConvertValue(_exampleValue);
            }
        }

        private string _name;

        public DataFileColumn()
        {

        }

        public DataFileColumn(string name): this()
        {
            Name = name;
        }

        public DataFileColumn(int index, string name):this(name)
        {
            Index = index;
        }

        public DataFileColumn(string name, int length)
            : this(name)
        {
            Length = length;
        }

        public int Length
        {
            get { return _length; }
            set
            {
                _length = value;
                PadName();
            }
        }

        public bool LengthSpecified
        {
            get { return _length > 0; }
        }

        public string Name
        {
            get { return _name; }
            set
            {
                _name = value;
                PadName();
            }
        }

        private void PadName()
        {
            if (!FixedWidthMode) return;
            if (_name != null)
            {
                _name = _name.PadRight(_length);
            }
        }

        public object ConvertValue(object value)
        {
            if (DataType == null) return value;
            var rtn = Activator.CreateInstance(DataType);
            if (value == rtn)
            {
                return rtn;
            }
            if (Convert.IsDBNull(value)) return rtn;
            var converter = TypeDescriptor.GetConverter(value.GetType());
            if (value.GetType() == DataType)
            {
                return value;
            }
            return converter.ConvertFrom(value);
        }
    }
}