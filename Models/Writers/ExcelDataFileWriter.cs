using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using DataFile.Interfaces;
using OfficeOpenXml;

namespace DataFile.Models.Writers
{
    class ExcelDataFileWriter: IDataFileWriter
    {
        public Stream Stream { get; set; }
        public DataFileLayout Layout { get; set; }
        private bool _open;
        private ExcelWorksheet _activeWorksheet;
        private int _position;
        private ExcelPackage _activeWriter;

        public ExcelDataFileWriter(Stream stream, DataFileLayout layout)
        {
            if (layout.Format != DataFileFormat.XLSX)
            {
                throw new NotSupportedException("Only the XLSX Format is supported");
            }
            Stream = stream;
            Layout = layout;
        }

        public void Open()
        {
            if (_open) return;
            _activeWriter = new ExcelPackage(Stream);
            _activeWorksheet = _activeWriter.Workbook.Worksheets.Add("Sheet 1");
            _open = true;
        }

        public void Write(IEnumerable<object> values)
        {
            var valuesList = values.ToList();
            try
            {
                for (var index = 0; index < valuesList.Count; index++)
                {
                    var value = valuesList[index];
                    _activeWorksheet.Cells[_position + 1, index + 1].Value = value;
                }
                _position++;
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
            _activeWriter.Save();
            _open = false;
        }

        public void Dispose()
        {
            Close();
            _activeWriter?.Dispose();
        }
    }
}
