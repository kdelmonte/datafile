using System;
using System.Collections.Generic;
using System.IO;
using DataFile.Interfaces;
using DataFile.Models.Writers;

namespace DataFile.Models
{
    public class DataFileWriter:IDataFileWriter
    {
        public Stream Stream { get; set; }
        public DataFileLayout Layout { get; set; }
        private bool _open;
        private IDataFileWriter _activeWriter;

        public DataFileWriter(Stream stream, DataFileLayout layout)
        {
            if (layout.Format == DataFileFormat.XLS)
            {
                throw new NotSupportedException("Writing to XLS format is not supported");
            }
            Stream = stream;
            Layout = layout;
        }

        public DataFileWriter(string path, DataFileLayout layout)
            : this(File.Open(path, FileMode.OpenOrCreate), layout)
        {
            
        }

        public void Open()
        {
            if (_open) return;
            switch (Layout.Format)
            {
                case DataFileFormat.XLS:
                case DataFileFormat.XLSX:
                    _activeWriter = new ExcelDataFileWriter(Stream, Layout);
                    break;
                default:
                    _activeWriter = new CharacterDelimitedDataFileWriter(Stream, Layout);
                    break;
            }
            _activeWriter.Open();
            _open = true;
        }

        public void Write(IEnumerable<object> values)
        {
            try
            {
                _activeWriter.Write(values);
            }
            catch
            {
                if (_activeWriter == null)
                {
                    throw new Exception("You must call Open() before attempting to write");
                }
                throw;
            }
        }

        public void Write(DataFileReader reader)
        {
            Write(reader.GetValues());
        }

        public void Close()
        {
            if (!_open) return;
            _activeWriter.Close();
            _open = false;
        }

        public void Dispose()
        {
            Close();
            _activeWriter?.Dispose();
        }
    }
}
