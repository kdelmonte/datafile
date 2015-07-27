using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using DataFile.Interfaces;
using DataFile.Models.Readers;
using Newtonsoft.Json.Linq;

namespace DataFile.Models
{
    public class DataFileReader: IDataFileReader
    {
        public string Path { get; set; }
        private DataFileLayout _layout;
        public DataFileLayout Layout
        {
            get { return _layout; }
            set
            {
                _layout = value;
                _columnsSpecified = _layout != null && _layout.Columns != null && _layout.Columns.Any();
            }
        }
        public int Depth
        {
            get
            {
                return 0;
            }
        }
        public int FieldCount
        {
            get
            {
                return _values.Length;
            }
        }
        public bool IsClosed
        {
            get
            {
                return !_open;
            }
        }
        public int RecordsAffected
        {
            get
            {
                return -1;
            }
        }
        public int Skip { get; set; }

        public string CurrentWorksheet
        {
            get 
            {
                if (_activeReader is ExcelDataFileReader)
                {
                    return ((ExcelDataFileReader)_activeReader).CurrentWorksheet; 
                }
                return null;
            }
        }

        public string TargetWorksheetName { get; set; }
        private bool _open;
        private IDataFileReader _activeReader;
        private bool _columnsSpecified;
        private object[] _values;

        public object this[string columnName]
        {
            get
            {
                var columnIndex = GetColumnIndex(columnName, true);
                return this[columnIndex];
            }
        }

        public object this[int columnIndex]
        {
            get { return _values[columnIndex]; }
        }

        public DataFileReader(string path, DataFileLayout layout = null)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("layout");
            }
            Path = path;
            Layout = layout;
        }

        public void Open()
        {
            if (_open) return;
            switch (Layout.Format)
            {
                case DataFileFormat.XLS:
                case DataFileFormat.XLSX:
                    _activeReader = new ExcelDataFileReader(Path, Layout){TargetWorksheetName = TargetWorksheetName};
                    break;
                case DataFileFormat.SpaceDelimited:
                    _activeReader = new SpaceDelimitedDataFileReader(Path, Layout);
                    break;
                default:
                    _activeReader = new CharacterDelimitedDataFileReader(Path, Layout);
                    break;
            }
            _activeReader.Open();
            if (Skip > 0)
            {
                var lineNumber = Skip;
                while (lineNumber > 0 && Read())
                {
                    lineNumber--;
                }
            }
            _open = true;
        }

        public bool Read()
        {
            try
            {
                var result = _activeReader.Read();
                var values = new object[_columnsSpecified ? Layout.Columns.Count : _activeReader.FieldCount];
                if (result)
                {
                    if (_columnsSpecified)
                    {
                        for (var index = 0; index < _activeReader.FieldCount; index++)
                        {
                            object value = null;
                            if (index < Layout.Columns.Count)
                            {
                                var textValue = _activeReader[index];
                                var column = Layout.Columns[index];
                                value = column.ConvertValue(textValue);
                            }
                            values[index] = value;
                        }
                    }
                    else
                    {
                        values = _activeReader.GetValues();
                    }
                    
                }
                _values = values;
                return result;
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

        public T As<T>()
        {
            if (Layout == null)
            {
                throw new Exception("A layout is required for this method");
            }
            var jsonObject = new JObject();
            foreach (var column in Layout.Columns)
            {
                jsonObject[column.Name] = new JObject(_values[column.Index]);
            }
            return jsonObject.ToObject<T>();
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
            var converter = TypeDescriptor.GetConverter(value.GetType());
            if (value is T)
            {
                return (T)value;
            }
            return (T)converter.ConvertFrom(value);
        }

        public T GetValue<T>(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetValue<T>(columnIndex);
        }

        public bool GetBoolean(int columnIndex)
        {
            return GetValue<bool>(columnIndex);
        }

        public bool GetBoolean(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetBoolean(columnIndex);
        }

        public byte GetByte(int columnIndex)
        {
            return GetValue<byte>(columnIndex);
        }

        public byte GetByte(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetByte(columnIndex);
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

        public long GetBytes(string columnName, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetBytes(columnIndex, fieldOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int columnIndex)
        {
            return GetValue<char>(columnIndex);
        }

        public char GetChar(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetChar(columnIndex);
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

        public long GetChars(string columnName, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetChars(columnIndex, fieldOffset, buffer, bufferOffset, length);
        }

        public IDataReader GetData(int columnIndex)
        {
            throw new NotSupportedException();
        }

        public Type GetFieldType(int columnIndex)
        {
            return this[columnIndex].GetType();
        }

        public Type GetFieldType(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetFieldType(columnIndex);
        }

        public string GetDataTypeName(int columnIndex)
        {
            return this[columnIndex].GetType().ToString();
        }

        public string GetDataTypeName(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDataTypeName(columnIndex);
        }

        public DateTime GetDateTime(int columnIndex)
        {
            return GetValue<DateTime>(columnIndex);
        }

        public DateTime GetDateTime(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDateTime(columnIndex);
        }

        public decimal GetDecimal(int columnIndex)
        {
            return GetValue<decimal>(columnIndex);
        }

        public decimal GetDecimal(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDecimal(columnIndex);
        }

        public double GetDouble(int columnIndex)
        {
            return GetValue<double>(columnIndex);
        }

        public double GetDouble(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDouble(columnIndex);
        }

        public float GetFloat(int columnIndex)
        {
            return GetValue<float>(columnIndex);
        }

        public float GetFloat(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetFloat(columnIndex);
        }

        public Guid GetGuid(int columnIndex)
        {
            return GetValue<Guid>(columnIndex);
        }

        public Guid GetGuid(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetGuid(columnIndex);
        }

        public short GetInt16(int columnIndex)
        {
            return GetValue<short>(columnIndex);
        }

        public short GetInt16(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt16(columnIndex);
        }

        public int GetInt32(int columnIndex)
        {
            return GetValue<int>(columnIndex);
        }

        public int GetInt32(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt32(columnIndex);
        }

        public long GetInt64(int columnIndex)
        {
            return GetValue<long>(columnIndex);
        }

        public long GetInt64(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt64(columnIndex);
        }

        public string GetName(int columnIndex)
        {
            if (Layout == null)
            {
                throw new Exception("A layout is required for this method");
            }
            return Layout.Columns[columnIndex].Name;
        }

        public int GetOrdinal(string columnName)
        {
            return GetColumnIndex(columnName, true);
        }

        public DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public string GetString(int columnIndex)
        {
            return GetValue<string>(columnIndex);
        }

        public string GetString(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetString(columnIndex);
        }

        public object GetValue(int columnIndex)
        {
            return this[columnIndex];
        }

        public object GetValue(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetValue(columnIndex);
        }

        public int GetValues(object[] values)
        {
            throw new NotSupportedException();
        }

        public object[] GetValues()
        {
            return _values;
        }

        public bool IsDBNull(int columnIndex)
        {
            return this[columnIndex] == DBNull.Value;
        }

        public bool IsDBNull(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return IsDBNull(columnIndex);
        }

        public bool NextResult()
        {
            return _activeReader.NextResult();
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
            if (_activeReader != null)
            {
                _activeReader.Dispose();
            }
        }
        private int GetColumnIndex(string columnName, bool throwExceptionifNotFound = false)
        {
            var columnIndex = -1;
            for (var index = 0; index < Layout.Columns.Count; index++)
            {
                var column = Layout.Columns[index];
                if (column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    columnIndex = index;
                }
            }
            if (throwExceptionifNotFound)
            {
                if (columnIndex == -1)
                {
                    throw new IndexOutOfRangeException("Column not found");
                }
            }
            return columnIndex;
        }
    }
}
