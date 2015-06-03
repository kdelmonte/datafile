using System.Collections.Generic;

namespace DataFile.Models
{
    public class Column
    {
        public string Alias;
        public int End;
        public string ExampleValue;
        public bool FixedWidthMode;
        public int Index;
        public int Start;
        private int _length;

        private string _name;

        public Column(string name)
        {
            Name = name;
        }

        public Column(int index, string name):this(name)
        {
            Index = index;
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