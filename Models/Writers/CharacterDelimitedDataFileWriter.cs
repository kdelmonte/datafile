using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using DataFile.Interfaces;

namespace DataFile.Models.Writers
{
    class CharacterDelimitedDataFileWriter:IDataFileWriter
    {
        public Stream Stream { get; set; }
        public DataFileLayout Layout { get; set; }
        private bool _open;
        private StreamWriter _activeWriter;

        public CharacterDelimitedDataFileWriter(Stream stream, DataFileLayout layout)
        {
            if (layout.Format == DataFileFormat.XLS || layout.Format == DataFileFormat.XLSX)
            {
                throw new NotSupportedException("Format is not supported. Try using the ExcelDataFileWriter");
            }
            Stream = stream;
            Layout = layout;
            Layout.TextQualifier = "\"";
        }

        public void Open()
        {
            if (_open) return;
            _activeWriter = new StreamWriter(Stream);
            _open = true;
        }

        private string NormalizeValue(object value)
        {
            var textValue = value.ToString();
            if (textValue.Contains(Layout.TextQualifier))
            {
                textValue = textValue.Contains(Layout.FieldDelimiter) ?
                    Layout.TextQualifier + textValue + Layout.TextQualifier : 
                    textValue;
            }
            return textValue;
        }

        public void Write(IEnumerable<object> values)
        {
            try
            {
                var line = string.Join(Layout.FieldDelimiter, values.Select(value => NormalizeValue(value.ToString())));
                _activeWriter.WriteLine(line);
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

        public void Write(IDataReader reader)
        {
            while (reader.Read())
            {
                var values = new List<object>();
                for (var x = 0; x < reader.FieldCount; x++)
                {
                    values.Add(reader.GetValue(x));
                }
                Write(values);
            }
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
