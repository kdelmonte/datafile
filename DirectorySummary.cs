using System.Collections.Generic;
using System.IO;

namespace Fable
{
    public class DirectorySummary
    {
        public string Name;
        public string Size;
        private long _length;

        public DirectorySummary()
        {
            Attributes = new Dictionary<string, object>();
        }

        public DirectorySummary(string path) : this()
        {
            SetProperties(new DirectoryInfo(path));
        }

        public DirectorySummary(DirectoryInfo directory) : this()
        {
            SetProperties(directory);
        }

        public Dictionary<string, object> Attributes { get; set; }
        public string ParentName { get; set; }
        public bool Exists { get; set; }
        public string Extension { get; set; }
        public string FullName { get; set; }

        public long Length
        {
            get { return _length; }
            set
            {
                _length = value;
                Size = Utilities.BytesToReadableSize(value);
            }
        }

        private void SetProperties(DirectoryInfo directory)
        {
            Name = directory.Name;
            ParentName = directory.Parent != null ? directory.Parent.Name : null;
            FullName = directory.FullName;
            Extension = directory.Extension;
            Length = (long) Utilities.CalculateFolderSize(directory);
            Exists = directory.Exists;
        }
    }
}