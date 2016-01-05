using System;
using System.Data;
using System.Linq;
using DataFile.Interfaces;
using DataFile.Models.Readers;

namespace DataFile.Models
{
    public class DataFileReader: IDataFileReader
    {
        public string Path { get; set; }
        public DataFileLayout Layout { get;}

        public int Depth => 0;

        public int FieldCount => CurrentRecord?.FieldCount ?? Layout.Columns.Count;

        public bool IsClosed => !_open;

        public int RecordsAffected => -1;
        public int StartAt { get; set; }

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
        public DataFileRecord CurrentRecord { get; private set; }
        private bool _open;
        private IDataFileReader _activeReader;
        private readonly bool _columnsSpecified;

        public object this[string columnName] => CurrentRecord[columnName];

        public object this[int columnIndex] => CurrentRecord[columnIndex];

        public DataFileReader(string path, DataFileLayout layout = null)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }
            Path = path;
            Layout = layout;
            _columnsSpecified = Layout?.Columns != null && Layout.Columns.Any();
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
            if (StartAt > 0)
            {
                var lineNumber = StartAt;
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
                var endOfFile = !_activeReader.Read();
                var values = new object[_columnsSpecified ? Layout.Columns.Count : _activeReader.FieldCount];
                if (!endOfFile)
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
                CurrentRecord = new DataFileRecord(Layout, values);
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

        public T As<T>()
        {
            return CurrentRecord.As<T>();
        }


        public T GetValue<T>(int columnIndex)
        {
            return CurrentRecord.GetValue<T>(columnIndex);
        }

        public T GetValue<T>(string columnName)
        {
            return CurrentRecord.GetValue<T>(columnName);
        }

        public bool GetBoolean(int columnIndex)
        {
            return CurrentRecord.GetBoolean(columnIndex);
        }

        public bool GetBoolean(string columnName)
        {
            return CurrentRecord.GetBoolean(columnName);
        }

        public byte GetByte(int columnIndex)
        {
            return CurrentRecord.GetByte(columnIndex);
        }

        public byte GetByte(string columnName)
        {
            return CurrentRecord.GetByte(columnName);
        }

        public long GetBytes(int columnIndex, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            return CurrentRecord.GetBytes(columnIndex, fieldOffset, buffer, bufferOffset, length);
        }

        public long GetBytes(string columnName, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            return CurrentRecord.GetBytes(columnName, fieldOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int columnIndex)
        {
            return CurrentRecord.GetChar(columnIndex);
        }

        public char GetChar(string columnName)
        {
            return CurrentRecord.GetChar(columnName);
        }

        public long GetChars(int columnIndex, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            return CurrentRecord.GetChars(columnIndex, fieldOffset, buffer, bufferOffset, length);
        }

        public long GetChars(string columnName, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            return CurrentRecord.GetChars(columnName, fieldOffset, buffer, bufferOffset, length);
        }

        public IDataReader GetData(int columnIndex)
        {
            throw new NotSupportedException();
        }

        public Type GetFieldType(int columnIndex)
        {
            return CurrentRecord.GetFieldType(columnIndex);
        }

        public Type GetFieldType(string columnName)
        {
            return CurrentRecord.GetFieldType(columnName);
        }

        public string GetDataTypeName(int columnIndex)
        {
            return CurrentRecord.GetDataTypeName(columnIndex);
        }

        public string GetDataTypeName(string columnName)
        {
            return CurrentRecord.GetDataTypeName(columnName);
        }

        public DateTime GetDateTime(int columnIndex)
        {
            return CurrentRecord.GetDateTime(columnIndex);
        }

        public DateTime GetDateTime(string columnName)
        {
            return CurrentRecord.GetDateTime(columnName);
        }

        public decimal GetDecimal(int columnIndex)
        {
            return CurrentRecord.GetDecimal(columnIndex);
        }

        public decimal GetDecimal(string columnName)
        {
            return CurrentRecord.GetDecimal(columnName);
        }

        public double GetDouble(int columnIndex)
        {
            return CurrentRecord.GetDouble(columnIndex);
        }

        public double GetDouble(string columnName)
        {
            return CurrentRecord.GetDouble(columnName);
        }

        public float GetFloat(int columnIndex)
        {
            return CurrentRecord.GetFloat(columnIndex);
        }

        public float GetFloat(string columnName)
        {
            return CurrentRecord.GetFloat(columnName);
        }

        public Guid GetGuid(int columnIndex)
        {
            return CurrentRecord.GetGuid(columnIndex);
        }

        public Guid GetGuid(string columnName)
        {
            return CurrentRecord.GetGuid(columnName);
        }

        public short GetInt16(int columnIndex)
        {
            return CurrentRecord.GetInt16(columnIndex);
        }

        public short GetInt16(string columnName)
        {
            return CurrentRecord.GetInt16(columnName);
        }

        public int GetInt32(int columnIndex)
        {
            return CurrentRecord.GetInt32(columnIndex);
        }

        public int GetInt32(string columnName)
        {
            return CurrentRecord.GetInt32(columnName);
        }

        public long GetInt64(int columnIndex)
        {
            return CurrentRecord.GetInt64(columnIndex);
        }

        public long GetInt64(string columnName)
        {
            return CurrentRecord.GetInt64(columnName);
        }

        public string GetName(int columnIndex)
        {
            return CurrentRecord.GetName(columnIndex);
        }

        public int GetOrdinal(string columnName)
        {
            return CurrentRecord.GetOrdinal(columnName);
        }

        public DataTable GetSchemaTable()
        {
            return CurrentRecord.GetSchemaTable();
        }

        public string GetString(int columnIndex)
        {
            return CurrentRecord.GetString(columnIndex);
        }

        public string GetString(string columnName)
        {
            return CurrentRecord.GetString(columnName);
        }

        public object GetValue(int columnIndex)
        {
            return CurrentRecord.GetValue(columnIndex);
        }

        public object GetValue(string columnName)
        {
            return CurrentRecord.GetValue(columnName);
        }

        public int GetValues(object[] values)
        {
            return CurrentRecord.GetValues(values);
        }

        public object[] GetValues()
        {
            return CurrentRecord.GetValues();
        }

        public bool IsDBNull(int columnIndex)
        {
            return CurrentRecord.IsDBNull(columnIndex);
        }

        public bool IsDBNull(string columnName)
        {
            return CurrentRecord.IsDBNull(columnName);
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
            _activeReader?.Dispose();
        }
        
    }
}
