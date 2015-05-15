using System.Collections.Generic;

namespace Fable
{
    public class Column
    {
        public string Alias;
        public int End;
        public string ExampleValue;
        public bool FixedWidthMode;
        public List<ColumnValueFrequency> FrequencyValues = new List<ColumnValueFrequency>();
        public int Index;
        public int Start;
        private int _length;

        private string _name;

        public Column(int index, string name)
        {
            Index = index;
            Name = name;
        }

        public Column()
        {
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