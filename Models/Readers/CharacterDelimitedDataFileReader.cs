using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using DataFile.Interfaces;

namespace DataFile.Models.Readers
{
    public class CharacterDelimitedDataFileReader: IDataFileReader
    {
        public string Path { get; set; }
        public DataFileLayout Layout { get; set; }
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
        private bool _open;
        private StreamReader _activeReader;
        private object[] _values;

        public object this[int columnIndex]
        {
            get { return _values[columnIndex]; }
        }

        public object this[string columnName]
        {
            get { throw new NotSupportedException(); }
        }

        public CharacterDelimitedDataFileReader(string path, DataFileLayout layout)
        {
            Path = path;
            Layout = layout;
        }

        public void Open()
        {
            if (_open) return;
            _activeReader = File.OpenText(Path);
            _open = true;
        }

        public bool Read()
        {
            try
            {
                var values = new List<object>();
                var line = _activeReader.ReadLine();
                var endOfFile = line == null;
                if (!endOfFile)
                {
                    values.AddRange(ExtractValues(line));
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
            var converter = TypeDescriptor.GetConverter(value.GetType());
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
            throw new NotSupportedException();
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
            throw new NotSupportedException();
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

        public bool NextResult()
        {
            return false;
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

        private IEnumerable<object> ExtractValues(string stringToSplit)
        {
            return ParseRow(stringToSplit)
                        .Select(RemoveTextQualifiers);
        }

        private IEnumerable<string> ParseRow(string r)
        {
            var resp = new List<string>();
            var cont = false;
            var cs = "";

            var c = r.Split(new[] { Layout.FieldDelimiter }, StringSplitOptions.None);

            foreach (var y in c)
            {
                var x = y.Trim();

                if (cont)
                {
                    // End of field
                    if (x.EndsWith(Layout.TextQualifier))
                    {
                        cs += Layout.FieldDelimiter + x.Substring(0, x.Length - 1);
                        resp.Add(cs);
                        cs = "";
                        cont = false;
                        continue;
                    }
                    // Field still not ended
                    cs += Layout.FieldDelimiter + x;
                    continue;
                }

                // Start of encapsulation but comma has split it into at least next field
                if (x.StartsWith(Layout.TextQualifier) && !x.EndsWith(Layout.TextQualifier) || x == Layout.TextQualifier)
                {
                    cont = true;
                    cs += x.Substring(1);
                    continue;
                }

                // Fully encapsulated with no comma within
                if (x.StartsWith(Layout.TextQualifier) && x.EndsWith(Layout.TextQualifier))
                {
                    var doubleTextQualifier = Layout.TextQualifier + Layout.TextQualifier;
                    var tripleTextQualifier = Layout.TextQualifier + Layout.TextQualifier + Layout.TextQualifier;
                    if ((x.EndsWith(doubleTextQualifier) && !x.EndsWith(tripleTextQualifier)) && x != doubleTextQualifier)
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

        private string RemoveTextQualifiers(string value)
        {
            if (Layout == null || string.IsNullOrWhiteSpace(Layout.TextQualifier) || string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            return value.Trim(Layout.TextQualifier.ToCharArray());
        }
    }
}
