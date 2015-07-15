using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Excel;

namespace DataFile.Models
{
    public class DataFileReader: IDataReader
    {
        public string TextQualifier { get; set; }
        public DataFileInfo DataFile { get; set; }
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
                return DataFile.Columns.Count;
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


        private bool _open;
        private readonly bool _readingExcel;
        private readonly object _activeReader;
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

        public DataFileReader(DataFileInfo dataFile, bool skipColumnRow = false)
        {
            DataFile = dataFile;
            switch (DataFile.Format)
            {
                case DataFileFormat.XLS:
                case DataFileFormat.XLSX:
                    _activeReader = GetExcelDataReader();
                    _readingExcel = true;
                    break;
                default:
                    _activeReader = GetFileReader();
                    _readingExcel = false;
                    break;
            }
            _open = true;
        }

        private T GetSourceReader<T>()
        {
            return (T) _activeReader;
        }

        public bool Read()
        {
            var values = new List<object>();
            bool endOfFile;
            if (_readingExcel)
            {
                var excelReader = GetSourceReader<IExcelDataReader>();
                endOfFile = !excelReader.Read();
                if (!endOfFile)
                {
                    foreach (var column in DataFile.Columns)
                    {
                        var value = ConvertToString(excelReader[column.Index]);
                        values.Add(value);
                    }
                }
            }
            else
            {
                var reader = GetSourceReader<StreamReader>();
                var line = reader.ReadLine();
                endOfFile = line == null;
                if (!endOfFile)
                {
                    values.AddRange(SplitByFormat(line).Select(ConvertToString));
                }
            }

            _values = values.ToArray();
            return false;
        }

        public T Get<T>(object o)
        {
            var rtn = default(T);
            if (o == (object)rtn)
            {
                return rtn;
            }
            if (Convert.IsDBNull(o)) return rtn;
            var converter = TypeDescriptor.GetConverter(o.GetType());
            if (o is T)
            {
                return (T)o;
            }
            return (T) converter.ConvertFrom(o);
        }

        public bool GetBoolean(int columnIndex)
        {
            return Get<bool>(columnIndex);
        }

        public bool GetBoolean(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetBoolean(columnIndex);
        }

        public byte GetByte(int columnIndex)
        {
            return Get<byte>(columnIndex);
        }

        public byte GetByte(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetByte(columnIndex);
        }

        public long GetBytes(int columnIndex, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            var b = Encoding.UTF8.GetBytes(Get<string>(columnIndex));

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
            return Get<char>(columnIndex);
        }

        public char GetChar(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetChar(columnIndex);
        }

        public long GetChars(int columnIndex, long fieldOffset, char[] buffer, int bufferOffset, int length)

        {
            var b = Get<string>(columnIndex).ToCharArray();

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
            return Get<DateTime>(columnIndex);
        }

        public DateTime GetDateTime(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDateTime(columnIndex);
        }

        public decimal GetDecimal(int columnIndex)
        {
            return Get<decimal>(columnIndex);
        }

        public decimal GetDecimal(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDecimal(columnIndex);
        }

        public double GetDouble(int columnIndex)
        {
            return Get<double>(columnIndex);
        }

        public double GetDouble(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetDouble(columnIndex);
        }

        public float GetFloat(int columnIndex)
        {
            return Get<float>(columnIndex);
        }

        public float GetFloat(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetFloat(columnIndex);
        }

        public Guid GetGuid(int columnIndex)
        {
            return Get<Guid>(columnIndex);
        }

        public Guid GetGuid(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetGuid(columnIndex);
        }

        public short GetInt16(int columnIndex)
        {
            return Get<short>(columnIndex);
        }

        public short GetInt16(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt16(columnIndex);
        }

        public int GetInt32(int columnIndex)
        {
            return Get<int>(columnIndex);
        }

        public int GetInt32(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt32(columnIndex);
        }

        public long GetInt64(int columnIndex)
        {
            return Get<long>(columnIndex);
        }

        public long GetInt64(string columnName)
        {
            var columnIndex = GetColumnIndex(columnName, true);
            return GetInt64(columnIndex);
        }

        public string GetName(int columnIndex)
        {
            return DataFile.Columns[columnIndex].Name;
        }

        public int GetOrdinal(string columnName)
        {
            return GetColumnIndex(columnName);
        }

        public DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public string GetString(int columnIndex)
        {
            return Get<string>(columnIndex);
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
            if (!_readingExcel) return false;
            var excelReader = GetSourceReader<IExcelDataReader>();
            return excelReader.NextResult();
        }

        public void Close()
        {
            if (_readingExcel)
            {
                var excelReader = GetSourceReader<IExcelDataReader>();
                excelReader.Close();
            }
            else
            {
                var reader = GetSourceReader<StreamReader>();
                reader.Close();
            }
            _open = false;
        }

        public void Dispose()
        {
            if (_readingExcel)
            {
                var excelReader = GetSourceReader<IExcelDataReader>();
                excelReader.Dispose();
            }
            else
            {
                var reader = GetSourceReader<StreamReader>();
                reader.Dispose();
            }
        }

        protected string[] SplitByFormat(string stringToSplit)
        {
            switch (DataFile.Format)
            {
                case DataFileFormat.CommaDelimited:
                    return ParseCsvRow(stringToSplit);
                case DataFileFormat.SpaceDelimited:
                    return
                        DataFile.Columns.Select(column => stringToSplit.Substring(column.Start, column.Length))
                            .ToArray();
                default:
                    return stringToSplit.Split(new[] { DataFile.FieldDelimeter }, StringSplitOptions.None);
            }
        }

        protected static string[] ParseCsvRow(string r)
        {
            var resp = new List<string>();
            var cont = false;
            var cs = "";

            var c = r.Split(new[] { ',' }, StringSplitOptions.None);

            foreach (var y in c)
            {
                var x = y.Trim();

                if (cont)
                {
                    // End of field
                    if (x.EndsWith("\""))
                    {
                        cs += "," + x.Substring(0, x.Length - 1);
                        resp.Add(cs);
                        cs = "";
                        cont = false;
                        continue;
                    }
                    // Field still not ended
                    cs += "," + x;
                    continue;
                }

                // Start of encapsulation but comma has split it into at least next field
                if (x.StartsWith("\"") && !x.EndsWith("\"") || x == "\"")
                {
                    cont = true;
                    cs += x.Substring(1);
                    continue;
                }

                // Fully encapsulated with no comma within
                if (x.StartsWith("\"") && x.EndsWith("\""))
                {
                    if ((x.EndsWith("\"\"") && !x.EndsWith("\"\"\"")) && x != "\"\"")
                    {
                        cont = true;
                        cs = x;
                        continue;
                    }

                    resp.Add(x.Substring(1, x.Length - 2));
                    continue;
                }


                // Non encapsulated complete field
                resp.Add(x);
            }


            return resp.ToArray();
        }

        protected string RemoveTextQualifiers(string value)
        {
            if (string.IsNullOrWhiteSpace(TextQualifier) || string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            return value.Trim(TextQualifier.ToCharArray());
        }

        private static string ConvertToString(object value)
        {
            return value == null ? string.Empty : value.ToString();
        }

        private StreamReader GetFileReader()
        {
            return File.OpenText(DataFile.FullName);
        }

        private IExcelDataReader GetExcelDataReader(bool skipColumnRow = false, bool onActiveWorksheet = true)
        {
            var stream = File.Open(DataFile.FullName, FileMode.Open, FileAccess.Read);
            var reader = DataFile.Format == DataFileFormat.XLSX
                                ? ExcelReaderFactory.CreateOpenXmlReader(stream)
                                : ExcelReaderFactory.CreateBinaryReader(stream);
            reader.IsFirstRowAsColumnNames = DataFile.HasColumnHeaders;
            if (onActiveWorksheet && !string.IsNullOrEmpty(DataFile.ActiveWorksheet))
            {
                var foundSheet = false;
                for (var x = 0; x < reader.ResultsCount; x++)
                {
                    if (reader.Name.Equals(DataFile.ActiveWorksheet))
                    {
                        foundSheet = true;
                        break;
                    }
                    reader.NextResult();
                }
                if (!foundSheet)
                {
                    throw new Exception(string.Format("The specified Excel Sheet \" {0} \" was not found", DataFile.ActiveWorksheet));
                }
            }

            if (skipColumnRow && DataFile.HasColumnHeaders)
            {
                reader.Read();
            }
            return reader;
        }

        private int GetColumnIndex(string columnName, bool throwExceptionifNotFound = false)
        {
            var columnIndex = -1;
            for (var index = 0; index < DataFile.Columns.Count; index++)
            {
                var column = DataFile.Columns[index];
                if (column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    columnIndex = index;
                }
            }
            if (throwExceptionifNotFound)
            {
                if (columnIndex == -1)
                {
                    throw new Exception("Column not found");
                }
            }
            return columnIndex;
        }

    }
}
