using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Text;
using DataFile.Interfaces;
using Excel;

namespace DataFile.Models.Readers
{
    public class ExcelDataFileReader: IDataFileReader
    {
        public string Path { get; set; }
        public DataFileLayout Layout { get; set; }
        public int Depth => 0;
        public int FieldCount => _values.Length;

        public bool IsClosed => !_open;

        public int RecordsAffected => -1;
        public string CurrentWorksheet { get; private set; }
        public string TargetWorksheetName { get; set; }
        private bool _open;
        private IExcelDataReader _activeReader;
        private object[] _values;

        public object this[string columnName]
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        public object this[int columnIndex] => _values[columnIndex];

        public ExcelDataFileReader(string path, DataFileLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }
            Path = path;
            Layout = layout;
        }

        public void Open()
        {
            if (_open) return;
            var stream = File.Open(Path, FileMode.Open, FileAccess.Read);
            var excelReader = Layout.Format == DataFileFormat.XLSX
                                ? ExcelReaderFactory.CreateOpenXmlReader(stream)
                                : ExcelReaderFactory.CreateBinaryReader(stream);
            excelReader.IsFirstRowAsColumnNames = Layout.HasColumnHeaders;

            if (!string.IsNullOrWhiteSpace(TargetWorksheetName))
            {
                var foundSheet = false;

                for (var x = 0; x < excelReader.ResultsCount; x++)
                {
                    if (excelReader.Name.Equals(TargetWorksheetName, StringComparison.OrdinalIgnoreCase))
                    {
                        foundSheet = true;
                        break;
                    }
                    excelReader.NextResult();
                }
                if (!foundSheet)
                {
                    throw new Exception($"The specified Worksheet \"{TargetWorksheetName}\" was not found");
                }
            }
            _activeReader = excelReader;
            CurrentWorksheet = _activeReader.Name;
            _open = true;
        }

        public bool Read()
        {
            try
            {
                var values = new List<object>();
                var endOfFile = !_activeReader.Read();
                if (!endOfFile)
                {
                    for (var index = 0; index < _activeReader.FieldCount; index++)
                    {
                        var value = ConvertToString(_activeReader[index]);
                        values.Add(value);
                    }
                }
                _values = values.ToArray();
                return !endOfFile;
            }
            catch
            {
                if (_activeReader == null)
                {
                    throw new Exception("You must call Open() before attempting to read");
                }
                throw;
            }
           
        }
        
        public T GetValue<T>(int columnIndex)
        {
            var value = _values[columnIndex];
            var rtn = default(T);
            if (value == (object)rtn)
            {
                return rtn;
            }
            if (Convert.IsDBNull(value)) return rtn;
            var converter = TypeDescriptor.GetConverter(typeof(T));
            if (value is T)
            {
                return (T) value;
            }
            return (T) converter.ConvertFrom(value);
        }

        public bool GetBoolean(int columnIndex)
        {
            return GetValue<bool>(columnIndex);
        }

        public byte GetByte(int columnIndex)
        {
            return GetValue<byte>(columnIndex);
        }

        public long GetBytes(int columnIndex, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            var b = Encoding.UTF8.GetBytes(GetValue<string>(columnIndex));

            if (bufferOffset >= b.Length)
            {
                return 0;
            }

            length = bufferOffset + length <= b.Length ? length : b.Length - bufferOffset;

            Array.Copy(b, bufferOffset, buffer, 0, length);

            return length;
        }

        public char GetChar(int columnIndex)
        {
            return GetValue<char>(columnIndex);
        }

        public long GetChars(int columnIndex, long fieldOffset, char[] buffer, int bufferOffset, int length)

        {
            var b = GetValue<string>(columnIndex).ToCharArray();

            if (bufferOffset >= b.Length)
            {
                return 0;
            }

            length = bufferOffset + length <= b.Length ? length : b.Length - bufferOffset;

            Array.Copy(b, bufferOffset, buffer, 0, length);

            return length;
        }

        public IDataReader GetData(int columnIndex)
        {
            return _activeReader.GetData(columnIndex);
        }

        public Type GetFieldType(int columnIndex)
        {
            return this[columnIndex].GetType();
        }

        public string GetDataTypeName(int columnIndex)
        {
            return this[columnIndex].GetType().ToString();
        }

        public DateTime GetDateTime(int columnIndex)
        {
            return GetValue<DateTime>(columnIndex);
        }

        public decimal GetDecimal(int columnIndex)
        {
            return GetValue<decimal>(columnIndex);
        }

        public double GetDouble(int columnIndex)
        {
            return GetValue<double>(columnIndex);
        }

        public float GetFloat(int columnIndex)
        {
            return GetValue<float>(columnIndex);
        }

        public Guid GetGuid(int columnIndex)
        {
            return GetValue<Guid>(columnIndex);
        }

        public short GetInt16(int columnIndex)
        {
            return GetValue<short>(columnIndex);
        }

        public int GetInt32(int columnIndex)
        {
            return GetValue<int>(columnIndex);
        }

        public long GetInt64(int columnIndex)
        {
            return GetValue<long>(columnIndex);
        }

        public string GetName(int columnIndex)
        {
            throw new NotSupportedException();
        }

        public int GetOrdinal(string columnName)
        {
            throw new NotSupportedException();
        }

        public DataTable GetSchemaTable()
        {
            return _activeReader.GetSchemaTable();
        }

        public string GetString(int columnIndex)
        {
            return GetValue<string>(columnIndex);
        }

        public object GetValue(int columnIndex)
        {
            return this[columnIndex];
        }

        public int GetValues(object[] values)
        {
            return _activeReader.GetValues(values);
        }

        public object[] GetValues()
        {
            return _values;
        }

        public bool IsDBNull(int columnIndex)
        {
            return this[columnIndex] == DBNull.Value;
        }

        public bool NextResult()
        {
            var result = _activeReader.NextResult();
            CurrentWorksheet = _activeReader.Name;
            return result;
        }

        public void Close()
        {
            if (!_open) return;
            _activeReader.Close();
            _open = false;
        }

        public void Dispose()
        {
            Close();
            _activeReader?.Dispose();
        }

        private static string ConvertToString(object value)
        {
            return value == null ? string.Empty : value.ToString();
        }
    }
}
