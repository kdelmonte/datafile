using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using DataFile.Interfaces;
using Newtonsoft.Json.Linq;

namespace DataFile.Models
{
    public class DataFileRecord: IDataFileRecord
    {
        private readonly object[] _values;
        public int FieldCount => _values.Length;
        public DataFileLayout Layout { get; }
        private bool _columnsSpecified;

        public object this[string columnName]
        {
            get
            {
                var columnIndex = GetColumnIndex(columnName, true);
                return this[columnIndex];
            }
        }

        public object this[int columnIndex] => _values[columnIndex];

        public DataFileRecord()
        {
            
        }

        public DataFileRecord(DataFileLayout layout, IEnumerable<object> values)
        {
            Layout = layout;
            _columnsSpecified = Layout?.Columns != null && Layout.Columns.Any();
            _values = values.ToArray();
        }

        public bool IsEmpty()
        {
           return _values.All(value => value == null || value.ToString().Trim().Length == 0);
        }

        public T As<T>()
        {
            if (Layout == null)
            {
                throw new Exception("A layout is required for this method");
            }
            var jsonObject = new JObject();
            for (var index = 0; index < Layout.Columns.Count; index++)
            {
                var column = Layout.Columns[index];
                jsonObject[column.Name] = new JObject(_values[index]);
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
            var converter = TypeDescriptor.GetConverter(typeof(T));
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
