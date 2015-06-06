using System.Collections.Generic;

namespace DataFile.Models
{
    public class DataFileColumn
    {
        public string Alias { get; set; }
        public int End { get; set; }
        public string ExampleValue { get; set; }
        public bool FixedWidthMode { get; set; }
        public int Index { get; set; }
        public int Start { get; set; }
        private int _length = -1;

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
    }
}