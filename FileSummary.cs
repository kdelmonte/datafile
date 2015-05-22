using System.Collections.Generic;
using System.IO;

namespace DataFile
{
    public class FileSummary
    {
        public Dictionary<string, object> Attributes = new Dictionary<string, object>();
        private long _length;

        public FileSummary()
        {
            Attributes = new Dictionary<string, object>();
        }

        public FileSummary(FileInfo file) : this()
        {
            SetProperties(file);
        }

        public FileSummary(string path) : this()
        {
            SetProperties(new FileInfo(path));
        }

        public string DirectoryName { get; set; }
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

        public string Name { get; set; }
        public string NameWithoutExtension { get; set; }
        public string Size { get; private set; }

        private void SetProperties(FileInfo file)
        {
            Name = file.Name;
            NameWithoutExtension = Path.GetFileNameWithoutExtension(file.FullName);
            DirectoryName = file.DirectoryName;
            FullName = file.FullName;
            Extension = file.Extension;
            Length = file.Length;

            Exists = file.Exists;
        }
    }
}