using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DataFile.DatabaseInterfaces;
using DataFile.Models;
using DataFile.Models.Query;
using Excel;

namespace DataFile
{
    public partial class DataFileInfo: IDisposable
    {
        
        // Properties
        //=================================================

        public delegate void OnInitializeHandler();

        public delegate void OnDatabaseSessionCloseHandler();

        public delegate void OnDatabaseSessionOpenHandler();

        private const int NumberOfExampleRows = 15;
        private const string DefaultColumnName = "Column";
        public int TotalRecords { get; private set; }
        private Layout _layout;
        public string UniqueIdentifier { get; private set; }

        public ColumnList AggregatableColumns { get; set; }
        public ColumnList Columns { get; set; }
        public string DirectoryName { get; set; }
        public bool EvaluatedEntirely { get; set; }
        public List<string> ExcelSheets { get; set; }
        public string Extension { get; set; }
        private string _fieldDelimeter;

        public string FieldDelimeter
        {
            get { return _fieldDelimeter; }
            set
            {
                _fieldDelimeter = value;
                Format = GetFileFormatByDelimeter(_fieldDelimeter);
            }
        }

        public int FirstDataRowIndex { get; set; }
        public Format Format { get; set; }

        public string FormatType
        {
            get { return ConvertKeywordToPhrase(Format.ToString()); }
        }

        public string FullName { get; set; }
        public bool HasColumnHeaders { get; set; }

        public bool IsFixedWidth
        {
            get { return Format == Format.SpaceDelimited; }
        }

        public long Length { get; set; }
        public string Name { get; set; }
        public string NameWithoutExtension { get; set; }
        public List<List<string>> SampleRows { get; set; }

        public string Size
        {
            get
            {
                return GetFileSize(Length);
            }
        }

        public Validity Validity { get; set; }
        public int Width { get; set; }
        public IDatabaseInterface DatabaseInterface { get; set; }

        public bool Exists
        {
            get { return File.Exists(FullName); }
        }

        public string ActiveWorksheet { get; private set; }

        public Layout Layout
        {
            get { return _layout; }
            set
            {
                _layout = value;
                if (_layout != null)
                {
                    Columns = _layout.Columns;
                }

            }
        }

        public bool KeepSessionOpenOnDispose { get; set; }

        public event OnInitializeHandler OnInitialize;
        public event OnDatabaseSessionOpenHandler OnDatabaseSessionOpen;
        public event OnDatabaseSessionCloseHandler OnDatabaseSessionClose;
        
        // Constructors/Destructors
        //=================================================

        public DataFileInfo(IDatabaseInterface dbInterface = null)
        {
            DatabaseInterface = dbInterface;
            UniqueIdentifier = Guid.NewGuid().ToString("N");
            HasColumnHeaders = true;
            AggregatableColumns = new ColumnList();
            SampleRows = new List<List<string>>();
            Validity = new Validity();
            InitializeColumnList();
        }

        public DataFileInfo(string filePath, IDatabaseInterface dbInterface = null)
            : this(dbInterface)
        {
            Initialize(filePath, true);
        }

        public DataFileInfo(string filePath, bool fileHasColumns, IDatabaseInterface dbInterface = null)
            : this(dbInterface)
        {
            Initialize(filePath, fileHasColumns);
        }

        public DataFileInfo(string source, Layout layout, IDatabaseInterface dbInterface = null)
            : this(dbInterface)
        {
            Layout = layout;
            Initialize(source, Layout != null && Layout.HasColumnHeaders);
        }

        ~DataFileInfo()
        {
            try
            {
                Dispose();
            }
            catch
            {

            }
        }

        
        // Public Methods
        //=================================================


        public void Dispose()
        {
            if (!DatabaseSessionActive || KeepSessionOpenOnDispose) return;
            CloseDatabaseSession();
            if (OnDatabaseSessionClose != null)
            {
                OnDatabaseSessionClose();
            }
        }

        public void SwitchToWorkSheet(int sheetIndex)
        {
            SwitchToWorkSheet(ExcelSheets[sheetIndex]);
        }

        public void SwitchToWorkSheet(string sheetName)
        {
            IExcelDataReader excelReader = null;
            Columns.Clear();
            SampleRows.Clear();
            ActiveWorksheet = sheetName;
            try
            {
                excelReader = GetExcelDataReader(true);
                if (HasColumnHeaders)
                {
                    excelReader.Read();
                    for (var x = 0; x < excelReader.FieldCount; x++)
                    {
                        var columnName = excelReader[x].ToString().Trim();
                        Columns.Add(new Column(x, columnName));
                    }
                }
                else
                {
                    for (var x = 0; x < excelReader.FieldCount; x++)
                    {
                        Columns.Add(new Column(x, DefaultColumnName + (x + 1)));
                    }
                }

                var i = 0;
                while (excelReader.Read())
                {
                    var fieldValues = new List<string>();
                    if (i >= NumberOfExampleRows)
                    {
                        break;
                    }
                    for (var x = 0; x < excelReader.FieldCount; x++)
                    {
                        var value = Convert.ToString(excelReader[x]);
                        fieldValues.Add(value);
                    }

                    SampleRows.Add(fieldValues);
                    i++;
                }
            }
            finally
            {
                if (excelReader != null) excelReader.Close();
            }
        }

        public void EvaluateEntirely()
        {
            TotalRecords = 0;
            if (IsFixedWidth && Layout != null)
            {
                EvaluatedEntirely = true;
                return;
            }
            IExcelDataReader excelReader = null;
            StreamReader reader = null;
            try
            {
                switch (Format)
                {
                    case Format.XLS:
                    case Format.XLSX:
                        excelReader = GetExcelDataReader();
                        while (excelReader.Read())
                        {
                            for (var x = 0; x < Columns.Count; x++)
                            {
                                var column = Columns[x];
                                var value = excelReader[column.Index].ToString();
                                if (value.Length > column.Length)
                                {
                                    column.Length = value.Length;
                                }
                                Columns[x] = column;
                            }
                            TotalRecords++;
                        }

                        break;
                    default:
                        reader = File.OpenText(FullName);
                        var line = reader.ReadLine();
                        if (HasColumnHeaders)
                        {
                            line = reader.ReadLine(); //Skip Columns
                        }
                        if (line != null)
                        {
                            while (line != null)
                            {
                                var fields = SplitByFormat(line);
                                if (fields.Length < Columns.Count)
                                {
                                    line = reader.ReadLine();
                                    continue;
                                }
                                for (var x = 0; x < Columns.Count; x++)
                                {
                                    var column = Columns[x];
                                    var value = TrimQuoteDelimeters(fields[column.Index]);
                                    //var value = fields[column.Index];
                                    if (value.Length > column.Length)
                                    {
                                        column.Length = value.Length;
                                    }
                                    Columns[x] = column;
                                }
                                line = reader.ReadLine();
                                TotalRecords++;
                            }
                        }
                        break;
                }
                foreach (var column in Columns.Where(column => column.Length == 0))
                {
                    column.Length = 1;
                }
                EvaluatedEntirely = true;
            }
            finally
            {
                if (reader != null) reader.Close();

                if (excelReader != null)
                {
                    excelReader.Close();
                }
            }
        }

        public void ChangeLayout(Layout layout, List<ColumnMapping> mappings = null)
        {
            Save(Format, null, true, layout, mappings);
        }

        public void ConvertTo(Format newFormat, Layout layout = null, List<ColumnMapping> mappings = null)
        {
            Save(newFormat, null, true, layout, mappings);
        }

        public DataFileInfo SaveAs(Format newFormat, string targetPath, bool overwrite = false, Layout layout = null, List<ColumnMapping> mappings = null)
        {
            Save(newFormat, targetPath, overwrite, layout, mappings);
            var newLfi = new DataFileInfo(targetPath, Layout);
            newLfi.CopyPropertiesFromOtherDataFile(this);
            return newLfi;
        }

        public DataFileInfo Copy(string targetPath, bool overwrite = false)
        {
            File.Copy(FullName, targetPath, overwrite);
            var newLfi = new DataFileInfo(targetPath, Layout);
            newLfi.CopyPropertiesFromOtherDataFile(this);
            return newLfi;
        }

        public static Format GetFileFormatByDelimeter(string delimeter)
        {
            switch (delimeter)
            {
                case "\t":
                    return Format.TabDelimited;
                case ",":
                    return Format.CommaDelimited;
                case "|":
                    return Format.PipeDelimited;
                case "":
                case " ":
                    return Format.SpaceDelimited;
                default:
                    return delimeter == ImportFieldDelimeter ? Format.DatabaseImport : Format.CharachterDelimited;
            }
        }

        public static string GetDelimeterByFileFormat(Format format)
        {
            switch (format)
            {
                case Format.TabDelimited:
                    return "\t";
                case Format.CommaDelimited:
                    return  ",";
                case Format.PipeDelimited:
                    return "|";
                case Format.SpaceDelimited:
                    return " ";
                case Format.DatabaseImport:
                    return ImportFieldDelimeter;
                default:
                    return null;
            }
        }

        public DataFileInfo QueryToFile(DatabaseCommand query = null, string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, HasColumnHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, DatabaseCommand query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, DatabaseCommand query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(targetFilePath, withHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, IEnumerable<Column> columns, DatabaseCommand query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, columns, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, IEnumerable<Column> columns, DatabaseCommand query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            var exportIsFixedWidth = newDelimeter != null && newDelimeter == string.Empty || IsFixedWidth;
            var overwritingCurrentFile = string.IsNullOrWhiteSpace(targetFilePath) || IsSamePath(FullName, targetFilePath);
            OpenDatabaseSession();
            DatabaseQueryToFile(query, targetFilePath, newDelimeter, grouplessRecordsOnly, groupId);
            var dataFileInfo = overwritingCurrentFile ? this : new DataFileInfo(targetFilePath, false);
            if (withHeaders)
            {
                dataFileInfo.InsertColumnHeaders();
            }
            return dataFileInfo;
        }

        public void InsertColumnHeaders()
        {
            if (HasColumnHeaders) return;
            var colString = GetFileColumnString();
            StreamWriter writer = null;
            StreamReader reader = null;
            try
            {
                var tempfile = Path.GetTempFileName();
                writer = new StreamWriter(tempfile);
                reader = new StreamReader(FullName);
                writer.WriteLine(colString);
                while (!reader.EndOfStream)
                    writer.WriteLine(reader.ReadLine());
                writer.Close();
                reader.Close();
                File.Copy(tempfile, FullName, true);
                try
                {
                    File.Delete(tempfile);
                }
                catch
                {
                }
                HasColumnHeaders = true;
            }
            finally
            {
                if (reader != null) reader.Close();
                if (writer != null) writer.Close();
            }
        }

        public void RemoveColumnHeaders()
        {
            if (!HasColumnHeaders) return;
            StreamWriter writer = null;
            StreamReader reader = null;
            try
            {
                var tempfile = Path.GetTempFileName();
                writer = new StreamWriter(tempfile);
                reader = new StreamReader(FullName);
                //Skip first line
                reader.ReadLine();
                while (!reader.EndOfStream)
                    writer.WriteLine(reader.ReadLine());
                writer.Close();
                reader.Close();
                File.Copy(tempfile, FullName, true);
                try
                {
                    File.Delete(tempfile);
                }
                catch
                {
                }
                HasColumnHeaders = false;
            }
            finally
            {
                if (reader != null) reader.Close();
                if (writer != null) writer.Close();
            }
        }

        public void ImportIntoTable(string targetConnectionString, string targetTable, DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            OpenDatabaseSession();
            DatabaseQueryToTable(targetConnectionString, targetTable, query, grouplessRecordsOnly, groupId);
        }

        public int UpdateRecords(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            OpenDatabaseSession();
            var recordsUpdated = DatabaseUpdate(query, grouplessRecordsOnly, groupId);
            UpdateFromDatabase();
            return recordsUpdated;
        }

        public int DeleteRecords(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            OpenDatabaseSession();
            var recordsDeleted = DatabaseDelete(query, grouplessRecordsOnly, groupId);
            UpdateFromDatabase();
            return recordsDeleted;
        }

        public void Shuffle()
        {
            var query = CreateDatabaseCommand();
            query.Shuffle();
            QueryToFile(query);
        }

        public void CopyPropertiesFromOtherDataFile(DataFileInfo source)
        {
            HasColumnHeaders = source.HasColumnHeaders;
            AggregatableColumns = source.AggregatableColumns;
            Layout = source.Layout;
        }

        public List<FileInfo> SplitByParts(int numberOfParts, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, newDirectory);
        }

        public List<FileInfo> SplitByParts(int numberOfParts, bool randomize)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, null);
        }

        public List<FileInfo> SplitByParts(int numberOfParts, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, maxSplits, newDirectory);
        }

        public List<FileInfo> SplitByMaxRecords(int maxRecords, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, newDirectory);
        }

        public List<FileInfo> SplitByMaxRecords(int maxRecords, bool randomize)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, null);
        }

        public List<FileInfo> SplitByMaxRecords(int maxRecords, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, maxSplits, newDirectory);
        }

        public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, bool randomize,
            string newDirectory)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, newDirectory);
        }

        public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, bool randomize)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, null);
        }

        public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, int maxSplits = 0,
            bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, maxSplits,
                newDirectory);
        }

        public List<FileInfo> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                null, newDirectory);
        }

        public List<FileInfo> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            bool randomize)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                null, null);
        }

        public List<FileInfo> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            int maxSplits = 0, bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                maxSplits, newDirectory);
        }

        public List<FileInfo> SplitByFileQuery(DatabaseCommand query, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                null, newDirectory);
        }

        public List<FileInfo> SplitByFileQuery(DatabaseCommand query, bool randomize)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                null, null);
        }

        public List<FileInfo> SplitByFileQuery(DatabaseCommand query, int maxSplits = 0, bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                maxSplits, newDirectory);
        }

        public List<FileInfo> SplitByFileSize(long maxBytes, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, newDirectory);
        }

        public List<FileInfo> SplitByFileSize(long maxBytes, bool randomize)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, null);
        }

        public List<FileInfo> SplitByFileSize(long maxBytes, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, maxSplits, newDirectory);
        }

        public List<List<FileInfo>> Split(List<SplitOption> options)
        {
            var rtnList = new List<List<FileInfo>>();

            for (var x = 0; x < options.Count; x++)
            {
                var splitOption = options[x];
                var exportDirectoryPath = DirectoryName + "Level_" + (x + 1) + "_" + splitOption.Method;
                var filesToSplit = rtnList.Count != 0
                    ? rtnList[rtnList.Count - 1]
                    : new List<FileInfo> { new FileInfo(FullName) };

                var levelSplits = new List<FileInfo>();
                foreach (var file in filesToSplit)
                {
                    levelSplits.AddRange(Split(splitOption.Method, splitOption.PrimaryValue,
                        splitOption.SecondaryValue, splitOption.Randomize, splitOption.MaxSplits,
                        exportDirectoryPath, file.FullName));
                }
                rtnList.Add(levelSplits);
            }
            rtnList.Reverse();
            return rtnList;
        }

        public void Delete()
        {
            File.Delete(FullName);
        }

        // Private Methods
        //=================================================

        private void InitializeExcelFile()
        {
            IExcelDataReader excelReader = null;
            try
            {
                excelReader = GetExcelDataReader();
                ExcelSheets = new List<string>();
                for (var x = 0; x < excelReader.ResultsCount; x++)
                {
                    ExcelSheets.Add(excelReader.Name);
                    excelReader.NextResult();
                }
                excelReader.Close();
                if (ExcelSheets.Any())
                {
                    SwitchToWorkSheet(0);
                }
            }
            finally
            {
                if (excelReader != null)
                {
                    excelReader.Close();
                }
            }
        }

        private void InitializeCharacterDelimitedFile()
        {
            StreamReader reader = null;
            try
            {
                reader = File.OpenText(FullName);
                var firstLine = reader.ReadLine();
                var rowCount = 0;
                var columns = SplitByFormat(firstLine);
                if (HasColumnHeaders)
                {
                    for (var i = 0; i < columns.Length; i++)
                    {
                        Columns.Add(new Column(i, columns[i]));
                    }
                }
                else
                {
                    for (var i = 0; i < columns.Length; i++)
                    {
                        Columns.Add(new Column(i, DefaultColumnName + (i + 1)));
                    }
                    SampleRows.Add(columns.ToList());
                    rowCount++;
                }
                while (rowCount < NumberOfExampleRows)
                {
                    var row = reader.ReadLine();
                    if (row == null)
                    {
                        break;
                    }
                    var fieldItems = SplitByFormat(row).ToList();
                    for (var i = 0; i < fieldItems.Count; i++)
                    {
                        fieldItems[i] = fieldItems[i].Trim();
                    }
                    SampleRows.Add(fieldItems);
                    rowCount++;
                }
            }
            finally
            {
                if (reader != null) reader.Close();
            }
        }

        private void InitializeSpaceDelimitedFile()
        {
            StreamReader reader = null;
            try
            {
                reader = File.OpenText(FullName);
                var firstLine = reader.ReadLine();
                var rowCount = 0;
                HasColumnHeaders = Layout != null && Layout.Columns.Count > 0;
                if (HasColumnHeaders)
                {
                    var columnLine = firstLine.ToLower();
                    foreach (var column in Columns)
                    {
                        if (columnLine.Substring(column.Start, column.Length).Trim() ==
                            column.Name.ToLower()) continue;
                        HasColumnHeaders = false;
                        break;
                    }
                }
                else
                {
                    Columns.Add(new Column(0, DefaultColumnName + 1));
                    SampleRows.Add(new List<string> { TrimQuoteDelimeters(firstLine) });
                    rowCount++;
                }
                Format = Format.SpaceDelimited;
                while (rowCount < NumberOfExampleRows)
                {
                    var row = reader.ReadLine();
                    if (row == null)
                    {
                        break;
                    }
                    SampleRows.Add(new List<string> { TrimQuoteDelimeters(row) });
                    rowCount++;
                }
            }
            finally
            {
                if (reader != null) reader.Close();
            }
        }

        private void DetermineFieldDelimeter()
        {
            StreamReader reader = null;
            try
            {
                reader = File.OpenText(FullName);
                var firstLine = reader.ReadLine();
                if (firstLine != null)
                {
                    if (Extension.ToLower() == ".csv")
                    {
                        FieldDelimeter = ",";
                    }
                    else
                    {
                        var delimeters = new[] { "\t", ",", "|", ImportFieldDelimeter };
                        foreach (var delimeter in delimeters)
                        {
                            if (!firstLine.Contains(delimeter)) continue;
                            FieldDelimeter = delimeter;
                            break;
                        }
                    }
                }
            }
            finally
            {
                if (reader != null) reader.Close();
            }
        }

        private void InitializeDelimitedFile()
        {
            DetermineFieldDelimeter();
            if (!IsFixedWidth)
            {
                InitializeCharacterDelimitedFile();
            }
            else
            {
                InitializeSpaceDelimitedFile();
            }
        }

        private void InitializeColumnProperties()
        {
            if (!IsFixedWidth)
            {
                for (var x = 0; x < Columns.Count; x++)
                {
                    var column = Columns[x];
                    for (var e = FirstDataRowIndex; e < SampleRows.Count; e++)
                    {
                        var row = SampleRows[e];
                        var value = TrimQuoteDelimeters(row[column.Index]);
                        if (string.IsNullOrEmpty(column.ExampleValue))
                        {
                            column.ExampleValue = value;
                        }
                        if (value.Length <= column.Length) continue;
                        column.Length = value.Length;
                    }
                    Columns[x] = column;
                }
            }
        }

        private void InitializeColumnList()
        {
            Columns = new ColumnList(ColumnList_CollectionChanged);
        }

        private void Initialize(string filePath, bool fileHasColumns)
        {
            HasColumnHeaders = fileHasColumns;
            var sourceFile = new FileInfo(filePath);
            if (!sourceFile.Exists) return;
            Length = sourceFile.Length;
            FullName = sourceFile.FullName;
            Name = sourceFile.Name;
            DirectoryName = sourceFile.DirectoryName + @"\";
            NameWithoutExtension = Path.GetFileNameWithoutExtension(FullName);
            Extension = sourceFile.Extension;
            var extension = Extension.ToLower().Replace(".", "");
            try
            {
                switch (extension)
                {
                    case "xls":
                    case "xlsx":
                        Format = extension == "xlsx" ? Format.XLSX : Format.XLS;
                        InitializeExcelFile();
                        break;
                    default:
                        InitializeDelimitedFile();
                        break;
                }

                if (SampleRows.Count == 0)
                {
                    Validity.AddWarning("File is empty");
                }
                foreach (var column in Columns.Where(column => column.Length == 0))
                {
                    column.Length = 1;
                }
                FirstDataRowIndex = SampleRows.Count > 1 ? 1 : 0;
                //Get Max Length of values in example rows
                //Set Example Values
                InitializeColumnProperties();
                if (OnInitialize != null)
                {
                    OnInitialize();
                }
            }
            catch (Exception ex)
            {
                Validity.Errors.Clear();
                Validity.AddError("Unexpected Error: " + ex.Message);
            }
        }

        private IExcelDataReader GetExcelDataReader(bool preventColumnRowSkip = false)
        {
            var stream = File.Open(FullName, FileMode.Open, FileAccess.Read);
            var reader = Format == Format.XLSX
                                ? ExcelReaderFactory.CreateOpenXmlReader(stream)
                                : ExcelReaderFactory.CreateBinaryReader(stream);
            reader.IsFirstRowAsColumnNames = HasColumnHeaders;
            if (!preventColumnRowSkip && HasColumnHeaders)
            {
                reader.Read();
            }
            return reader;
        }

        protected string ModifyValueBasedOnSettings(string value)
        {
            var forceRemovalOfQuotes = false;
            switch (Format)
            {
                case Format.SpaceDelimited:
                case Format.DatabaseImport:
                case Format.XLSX:
                case Format.XLS:
                    forceRemovalOfQuotes = true;
                    break;
            }
            return ModifyValueBasedOnSettings(value, Format, forceRemovalOfQuotes);
        }

        protected string ModifyValueBasedOnSettings(string value, bool forceRemovalOfQuotes)
        {
            return ModifyValueBasedOnSettings(value, Format, forceRemovalOfQuotes);
        }

        protected string ModifyValueBasedOnSettings(string value, Format type, bool forceRemovalOfQuotes)
        {
            if (type != Format.SpaceDelimited)
            {
                value = value.Trim();
            }
            value = PlaceQuoteDelimetersIfNeeded(value);
            if (forceRemovalOfQuotes)
            {
                value = TrimQuoteDelimeters(value);
            }
            return value;
        }

        protected static string TrimQuoteDelimeters(string value)
        {
            return value.Trim('"');
        }

        protected string PlaceQuoteDelimetersIfNeeded(string value)
        {
            if (Format == Format.SpaceDelimited || Format == Format.XLSX || Format == Format.XLS || HasQuoteDelimeters(value)) return value;
            return value.Contains(FieldDelimeter) ? "\"" + value + "\"" : value;
        }

        protected static bool HasQuoteDelimeters(string value)
        {
            const string doubleQuote = "\"";
            value = value.Trim();
            return value.StartsWith(doubleQuote) && value.EndsWith(doubleQuote);
        }

        protected string[] SplitByFormat(string stringToSplit)
        {
            switch (Format)
            {
                case Format.CommaDelimited:
                    return ParseCsvRow(stringToSplit);
                default:
                    return stringToSplit.Split(new[] {FieldDelimeter}, StringSplitOptions.None);
            }
        }

        protected static string[] ParseCsvRow(string r)
        {
            var resp = new List<string>();
            var cont = false;
            var cs = "";

            var c = r.Split(new[] {','}, StringSplitOptions.None);

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

        protected static bool LineIsEmpty(string line, string delimeter)
        {
            return string.IsNullOrEmpty(line) || line.Replace(delimeter, "").Trim().Length == 0;
        }

        protected void Save(Format newFormat, string newFilePath, bool overwrite, Layout newFixedWidthLayout, List<ColumnMapping> mappings)
        {
            var replacingCurrentFile = (newFilePath == null || newFilePath.ToLower().Trim() == FullName.ToLower().Trim());
            StreamReader reader = null;
            IExcelDataReader excelReader = null;
            if (IsFixedWidth)
            {
                if (Layout == null)
                {
                    throw new Exception("The current Fixed Width File has no layout. Please specify and try again.");
                }
            }
            var newDelimeter = "";
            var newExtension = ".txt";
            var forceRemovalOfQuotes = false;
            const string tempExtension = ".temp";
            var eligibleForLayoutChange = mappings != null && mappings.Count > 0 && newFixedWidthLayout != null;
            switch (newFormat)
            {
                case Format.CommaDelimited:
                    newDelimeter = ",";
                    newExtension = ".csv";
                    break;
                case Format.TabDelimited:
                    newDelimeter = "\t";
                    break;
                case Format.SpaceDelimited:
                    if (!eligibleForLayoutChange)
                    {
                        throw new Exception(
                            "A lead Layout and field to field mappings are required to save as space delimited");
                    }
                    newDelimeter = "";
                    forceRemovalOfQuotes = true;
                    break;
                case Format.PipeDelimited:
                    newDelimeter = "|";
                    break;
                case Format.DatabaseImport:
                    newDelimeter = ImportFieldDelimeter;
                    forceRemovalOfQuotes = true;
                    newExtension = DatabaseImportFileExtension;
                    break;
                case Format.XLS:
                case Format.XLSX:
                    throw new Exception("Converting to Excel is not supported at the moment. Please use another format");
            }
            if (replacingCurrentFile)
            {
                newFilePath = FullName.Replace(Extension, newExtension);
                newFilePath += tempExtension;
            }
            var writer = new StreamWriter(newFilePath);
            try
            {
                if (HasColumnHeaders)
                {
                    writer.WriteLine(!eligibleForLayoutChange
                        ? GetFileColumnString(newFormat, Columns)
                        // ReSharper disable once PossibleNullReferenceException
                        : GetFileColumnString(newFormat, newFixedWidthLayout.Columns));
                }
                string line;
                if (!eligibleForLayoutChange)
                {
                    switch (Format)
                    {
                        case Format.XLSX:
                        case Format.XLS:
                            excelReader = GetExcelDataReader();
                            while (excelReader.Read())
                            {
                                var row = new List<string>();
                                for (var y = 0; y < Columns.Count; y++)
                                {
                                    var field = Convert.ToString(excelReader[y]);
                                    row.Add(ModifyValueBasedOnSettings(field, newFormat, forceRemovalOfQuotes));
                                }
                                var rowString = string.Join(newDelimeter, row);
                                if (!LineIsEmpty(rowString, newDelimeter))
                                    writer.WriteLine(rowString);
                            }
                            break;
                        case Format.SpaceDelimited:
                            reader = File.OpenText(FullName);
                            line = reader.ReadLine();
                            if (HasColumnHeaders)
                            {
                                line = reader.ReadLine(); //Skip First Row
                            }

                            if (line != null)
                            {
                                while (line != null)
                                {
                                    var row = new List<string>();
                                    foreach (var column in Columns)
                                    {
                                        var value = Substr(line, column.Start, column.Length);
                                        value = ModifyValueBasedOnSettings(value, newFormat, forceRemovalOfQuotes);
                                        row.Add(value);
                                    }
                                    var rowString = string.Join(newDelimeter, row);
                                    if (!LineIsEmpty(rowString, newDelimeter))
                                        writer.WriteLine(rowString);
                                    line = reader.ReadLine();
                                }
                            }
                            break;
                        default:
                            reader = File.OpenText(FullName);
                            line = reader.ReadLine();
                            if (HasColumnHeaders)
                            {
                                line = reader.ReadLine(); //Skip First Row
                            }

                            if (line != null)
                            {
                                while (line != null)
                                {
                                    var fields = SplitByFormat(line);
                                    var row = new List<string>();
                                    foreach (var value in fields)
                                    {
                                        row.Add(ModifyValueBasedOnSettings(value, newFormat, forceRemovalOfQuotes));
                                    }
                                    var rowString = string.Join(newDelimeter, row);
                                    if (!LineIsEmpty(rowString, newDelimeter))
                                        writer.WriteLine(rowString);
                                    line = reader.ReadLine();
                                }
                            }
                            break;
                    }
                }
                else
                {
                    switch (Format)
                    {
                        case Format.XLS:
                        case Format.XLSX:
                            excelReader = GetExcelDataReader();
                            if (excelReader != null)
                            {
                                if (newFormat != Format.SpaceDelimited)
                                {
                                    while (excelReader.Read())
                                    {
                                        var row = new List<string>();
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (Columns.Count > mapping.SourceFieldIndex)
                                                {
                                                    var value = excelReader[mapping.SourceFieldIndex].ToString();
                                                    row.Add(ModifyValueBasedOnSettings(value, newFormat, forceRemovalOfQuotes));
                                                    row.Add(value);
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add("");
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                    }
                                }
                                else
                                {
                                    while (excelReader.Read())
                                    {
                                        var row = new List<string>();
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (Columns.Count > mapping.SourceFieldIndex)
                                                {
                                                    var value = excelReader[mapping.SourceFieldIndex].ToString();
                                                    row.Add(ModifyValueBasedOnColumnLength(targetColumn, value));
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add(ModifyValueBasedOnColumnLength(targetColumn, ""));
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                    }
                                }
                                excelReader.Close();
                            }
                            break;
                        case Format.SpaceDelimited:
                            reader = File.OpenText(FullName);
                            line = reader.ReadLine();
                            if (HasColumnHeaders)
                            {
                                line = reader.ReadLine(); //Skip First Row
                            }

                            if (line != null)
                            {
                                if (newFormat != Format.SpaceDelimited)
                                {
                                    while (line != null)
                                    {
                                        var row = new List<string>();
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (Columns.Count > mapping.SourceFieldIndex)
                                                {
                                                    var sourceColumn = Columns[mapping.SourceFieldIndex];
                                                    var value = Substr(line, sourceColumn.Start, sourceColumn.Length);
                                                    value = ModifyValueBasedOnSettings(value, newFormat, forceRemovalOfQuotes);
                                                    row.Add(value);
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add("");
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                        line = reader.ReadLine();
                                    }
                                }
                                else
                                {
                                    while (line != null)
                                    {
                                        var row = new List<string>();
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (Columns.Count > mapping.SourceFieldIndex)
                                                {
                                                    var sourceColumn = Columns[mapping.SourceFieldIndex];
                                                    var value = Substr(line, sourceColumn.Start, sourceColumn.Length);
                                                    value = ModifyValueBasedOnColumnLength(targetColumn, value);
                                                    row.Add(value);
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add(ModifyValueBasedOnColumnLength(targetColumn, ""));
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                        line = reader.ReadLine();
                                    }
                                }
                            }
                            break;
                        default:
                        {
                            reader = File.OpenText(FullName);
                            line = reader.ReadLine();
                            if (HasColumnHeaders)
                            {
                                line = reader.ReadLine(); //Skip First Row
                            }

                            if (line != null)
                            {
                                if (newFormat != Format.SpaceDelimited)
                                {
                                    while (line != null)
                                    {
                                        var row = new List<string>();
                                        var fields = SplitByFormat(line);
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (fields.Length > mapping.SourceFieldIndex)
                                                {
                                                    var value = fields[mapping.SourceFieldIndex];
                                                    value = ModifyValueBasedOnSettings(value, newFormat, forceRemovalOfQuotes);
                                                    row.Add(value);
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add("");
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                        line = reader.ReadLine();
                                    }
                                }
                                else
                                {
                                    while (line != null)
                                    {
                                        var row = new List<string>();
                                        var fields = SplitByFormat(line);
                                        foreach (var targetColumn in newFixedWidthLayout.Columns)
                                        {
                                            var foundMap = false;
                                            foreach (var mapping in mappings)
                                            {
                                                if (mapping.TargetFieldIndex != targetColumn.Index) continue;
                                                if (fields.Length > mapping.SourceFieldIndex)
                                                {
                                                    var value = fields[mapping.SourceFieldIndex];
                                                    value = ModifyValueBasedOnColumnLength(targetColumn, value);
                                                    row.Add(value);
                                                    foundMap = true;
                                                }
                                                break;
                                            }
                                            if (!foundMap)
                                            {
                                                row.Add(ModifyValueBasedOnColumnLength(targetColumn, ""));
                                            }
                                        }
                                        var rowString = string.Join(newDelimeter, row);
                                        if (!LineIsEmpty(rowString, newDelimeter))
                                            writer.WriteLine(rowString);
                                        line = reader.ReadLine();
                                    }
                                }
                            }
                        }
                            break;
                    }
                }
                writer.Close();
                if (reader != null) reader.Close();

                if (excelReader != null)
                {
                    excelReader.Close();
                }
                var finalPath = newFilePath.Replace(tempExtension, "").Replace(Extension, newExtension);
                if (overwrite)
                {
                    var finalFile = new FileInfo(finalPath);
                    if (finalFile.Exists)
                    {
                        finalFile.Delete();
                    }
                }
                if (!replacingCurrentFile) return;
                File.Delete(FullName);
                File.Move(newFilePath, finalPath);
                FullName = finalPath;
                FieldDelimeter = newDelimeter;
                Format = newFormat;
                Extension = newExtension;
                var fi = new FileInfo(FullName);
                Name = fi.Name;
                NameWithoutExtension = Path.GetFileNameWithoutExtension(FullName);
                Length = fi.Length;
                if (eligibleForLayoutChange)
                {
                    Layout = newFixedWidthLayout;
                }
            }
            finally
            {
                writer.Close();
                if (reader != null) reader.Close();

                if (excelReader != null)
                {
                    excelReader.Close();
                    excelReader.Dispose();
                }
            }
        }

        protected void UpdateFromDatabase()
        {
            QueryToFile(FullName, HasColumnHeaders);
        }

        protected List<FileInfo> Split(SplitMethod splitBy, object valueA, object valueB,
            bool randomize,
            object maxSplits, string newDirectory, string filePath = null)
        {
            var file = string.IsNullOrEmpty(filePath)
                ? this
                : !IsFixedWidth ? new DataFileInfo(filePath, HasColumnHeaders) : new DataFileInfo(filePath, Layout);
            if (!string.IsNullOrEmpty(newDirectory))
            {
                var newdir = new DirectoryInfo(newDirectory);
                if (!newdir.Exists)
                {
                    newdir.Create();
                }
                if (!newDirectory.EndsWith(@"\"))
                {
                    newDirectory += @"\";
                }
            }
            var query = CreateDatabaseCommand();
            var fileList = new List<string>();
            const string partSuffix = "_Part";
            const string incrementPlaceHolder = "[increment]";
            const string countColumnName = "____COUNT";
            var templateFileName = (string.IsNullOrEmpty(newDirectory) ? file.DirectoryName + @"\" : newDirectory) +
                                   file.NameWithoutExtension + partSuffix + incrementPlaceHolder + file.Extension;
            SqlDataReader dr = null;
            StreamReader reader = null;
            StreamWriter writer = null;
            try
            {
                string targetFileName;
                string line;
                var increment = 1;
                string sqlColString;
                long recordCount;
                var columnLine = file.GetFileColumnString();
                switch (splitBy)
                {
                    case SplitMethod.ByField:
                        file.OpenDatabaseSession();
                        var columnsWithAliases = valueA.ToString();
                        var columnsWithoutAliases = valueB.ToString();
                        sqlColString = columnsWithAliases + ", COUNT(*) AS '" + countColumnName + "'";
                        targetFileName = GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
                        writer = new StreamWriter(targetFileName);
                        fileList.Add(targetFileName);
                        reader = File.OpenText(file.FullName);
                        if (file.HasColumnHeaders)
                        {
                            writer.WriteLine(columnLine);
                            reader.ReadLine(); //Skip columns
                        }
                        reader.Close();
                        query.Select(sqlColString).GroupBy(columnsWithoutAliases).OrderBy("COUNT(*)");
                        var counts = file.GetDataTable(query);
                        for (var x = 0; x < counts.Columns.Count; x++)
                        {
                            var column = counts.Columns[x];
                            if (column.ColumnName != countColumnName) continue;
                            counts.Columns.RemoveAt(x);
                            break;
                        }
                        for (var split = 0; split < counts.Rows.Count; split++)
                        {
                            var row = counts.Rows[split];
                            var where = "";
                            for (var x = 0; x < counts.Columns.Count; x++)
                            {
                                var column = counts.Columns[x];
                                var value = row[x].ToString();
                                where += column.ColumnName + " = '" + value + "'";
                                if (x != counts.Columns.Count - 1)
                                {
                                    where += " AND ";
                                }
                            }

                            query.Where(where);
                            if (randomize)
                            {
                                query.Shuffle();
                            }
                            dr = file.GetDataReader(query);
                            while (dr.Read())
                            {
                                var rowString = "";
                                for (var y = 0; y < file.Columns.Count; y++)
                                {
                                    rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                    if (y != file.Columns.Count - 1)
                                    {
                                        rowString += file.FieldDelimeter;
                                    }
                                }
                                writer.WriteLine(rowString);
                            }
                            dr.Close();
                            if ((maxSplits != null && increment >= Convert.ToInt32(maxSplits)) || split == counts.Rows.Count - 1) continue;
                            increment++;
                            targetFileName =
                                GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                    increment.ToString()));
                            writer.Flush();
                            writer.Close();
                            writer = new StreamWriter(targetFileName);
                            fileList.Add(targetFileName);
                            if (file.HasColumnHeaders)
                            {
                                writer.WriteLine(columnLine);
                            }
                        }
                        break;
                    case SplitMethod.ByFileQuery:
                        var whereClause = valueA.ToString();
                        file.OpenDatabaseSession();
                        targetFileName =
                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
                        writer = new StreamWriter(targetFileName);
                        fileList.Add(targetFileName);
                        reader = File.OpenText(file.FullName);
                        if (file.HasColumnHeaders)
                        {
                            writer.WriteLine(columnLine);
                            reader.ReadLine(); //Skip columns
                        }
                        reader.Close();
                        query.Where(whereClause);
                        if (randomize)
                        {
                            query.Shuffle();
                        }
                        dr = file.GetDataReader(query, true, Guid.NewGuid().ToString("N"));
                        while (dr.Read())
                        {
                            var rowString = "";
                            for (var y = 0; y < file.Columns.Count; y++)
                            {
                                rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                if (y != file.Columns.Count - 1)
                                {
                                    rowString += file.FieldDelimeter;
                                }
                            }
                            writer.WriteLine(rowString);
                        }
                        dr.Close();
                        writer.Flush();
                        writer.Close();

                        if (randomize)
                        {
                            query.Shuffle();
                        }
                        dr = file.GetDataReader(query, true, Guid.NewGuid().ToString("N"));

                        if (dr.Read())
                        {
                            increment++;
                            targetFileName =
                                GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                    increment.ToString()));
                            writer = new StreamWriter(targetFileName);
                            fileList.Add(targetFileName);
                            if (file.HasColumnHeaders)
                            {
                                writer.WriteLine(columnLine);
                            }
                            do
                            {
                                var rowString = "";
                                for (var y = 0; y < file.Columns.Count; y++)
                                {
                                    rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                    if (y != file.Columns.Count - 1)
                                    {
                                        rowString += file.FieldDelimeter;
                                    }
                                }
                                writer.WriteLine(rowString);
                            } while (dr.Read());
                        }
                        break;
                    case SplitMethod.ByFileSize:
                        var maxFileSize = Convert.ToInt64(valueA);
                        targetFileName =
                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
                        writer = new StreamWriter(targetFileName);
                        fileList.Add(targetFileName);
                        reader = File.OpenText(file.FullName);
                        long fileSize = 0;
                        long columnLineSize = 0;
                        if (file.HasColumnHeaders)
                        {
                            columnLineSize = Encoding.UTF8.GetByteCount(columnLine);
                            fileSize += columnLineSize;
                            writer.WriteLine(columnLine);
                            reader.ReadLine(); //Skip columns
                        }
                        if (randomize)
                        {
                            reader.Close();
                            file.OpenDatabaseSession();
                            query.Shuffle();
                            
                            dr = file.GetDataReader(query);
                            while (dr.Read())
                            {
                                var rowString = "";
                                for (var y = 0; y < file.Columns.Count; y++)
                                {
                                    rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                    if (y != file.Columns.Count - 1)
                                    {
                                        rowString += file.FieldDelimeter;
                                    }
                                }
                                var rowSize = Encoding.UTF8.GetByteCount(rowString);
                                fileSize += rowSize;
                                if (fileSize <= maxFileSize ||
                                    (maxSplits != null && increment >= Convert.ToInt32(maxSplits)))
                                {
                                    writer.WriteLine(rowString);
                                }
                                else
                                {
                                    fileSize = 0;
                                    increment++;
                                    targetFileName =
                                        GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                            increment.ToString()));
                                    writer.Flush();
                                    writer.Close();
                                    writer = new StreamWriter(targetFileName);
                                    fileList.Add(targetFileName);
                                    if (file.HasColumnHeaders)
                                    {
                                        writer.WriteLine(columnLine);
                                        fileSize += columnLineSize;
                                    }
                                    writer.WriteLine(rowString);
                                    fileSize += rowSize;
                                }
                            }
                        }
                        else
                        {
                            line = reader.ReadLine();
                            while (line != null)
                            {
                                var rowSize = Encoding.UTF8.GetByteCount(line);
                                fileSize += rowSize;
                                if (fileSize <= maxFileSize ||
                                    (maxSplits != null && increment >= Convert.ToInt32(maxSplits)))
                                {
                                    writer.WriteLine(line);
                                }
                                else
                                {
                                    fileSize = 0;
                                    increment++;
                                    targetFileName =
                                        GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                            increment.ToString()));
                                    writer.Flush();
                                    writer.Close();
                                    writer = new StreamWriter(targetFileName);
                                    fileList.Add(targetFileName);
                                    if (file.HasColumnHeaders)
                                    {
                                        writer.WriteLine(columnLine);
                                        fileSize += columnLineSize;
                                    }
                                    writer.WriteLine(line);
                                    fileSize += rowSize;
                                }
                                line = reader.ReadLine();
                            }
                        }
                        break;
                    case SplitMethod.ByMaxRecords:
                        var maxRecords = Convert.ToInt32(valueA);
                        recordCount = 0;
                        targetFileName =
                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
                        writer = new StreamWriter(targetFileName);
                        fileList.Add(targetFileName);
                        if (file.HasColumnHeaders)
                        {
                            writer.WriteLine(columnLine);
                        }
                        reader = File.OpenText(file.FullName);
                        if (randomize)
                        {
                            reader.Close();
                            file.OpenDatabaseSession();
                            query.Shuffle();
                            dr = file.GetDataReader(query);
                            while (dr.Read())
                            {
                                var rowString = "";
                                for (var y = 0; y < file.Columns.Count; y++)
                                {
                                    rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                    if (y != file.Columns.Count - 1)
                                    {
                                        rowString += file.FieldDelimeter;
                                    }
                                }
                                writer.WriteLine(rowString);
                                recordCount++;
                                if (recordCount < maxRecords ||
                                    (maxSplits != null && increment >= Convert.ToInt32(maxSplits))) continue;
                                recordCount = 0;
                                increment++;
                                targetFileName =
                                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                        increment.ToString()));
                                writer.Flush();
                                writer.Close();
                                writer = new StreamWriter(targetFileName);
                                fileList.Add(targetFileName);
                                if (file.HasColumnHeaders)
                                {
                                    writer.WriteLine(columnLine);
                                }
                            }
                        }
                        else
                        {
                            if (file.HasColumnHeaders)
                            {
                                reader.ReadLine(); //Skip Columns
                            }
                            line = reader.ReadLine();
                            while (line != null)
                            {
                                writer.WriteLine(line);
                                line = reader.ReadLine();
                                recordCount++;
                                if (recordCount < maxRecords ||
                                    (maxSplits != null && increment >= Convert.ToInt32(maxSplits))) continue;
                                recordCount = 0;
                                increment++;
                                targetFileName =
                                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                        increment.ToString()));
                                writer.Flush();
                                writer.Close();
                                writer = new StreamWriter(targetFileName);
                                fileList.Add(targetFileName);
                                if (file.HasColumnHeaders)
                                {
                                    writer.WriteLine(columnLine);
                                }
                            }
                        }
                        break;
                    case SplitMethod.ByPercentage:
                        file.OpenDatabaseSession();
                        var percentage = Convert.ToDouble(valueA);
                        var partitionField = valueB.ToString();
                        var fileSplits = Convert.ToInt32(Math.Floor(100/percentage)).ToString();
                        var sqlColumnString = "TODO";
                        var randomizer = "NEWID()";
                        sqlColString = sqlColumnString + ",NTILE(" + fileSplits + ") OVER(PARTITION BY " +
                                       partitionField + " ORDER BY " +
                                       (randomize ? randomizer : "iFileSessionRecordId") + " DESC) AS FileGroup";
                        query.Select(sqlColString).GroupBy("FileGroup");
                        dr = file.GetDataReader(query);
                        var fileGroupColumnIndex = file.Columns.Count;
                        var currGroup = "";
                        while (dr.Read())
                        {
                            var fileGroup = dr[fileGroupColumnIndex].ToString();
                            if (currGroup != fileGroup && (maxSplits == null || increment <= Convert.ToInt32(maxSplits)))
                            {
                                targetFileName =
                                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                        increment.ToString()));
                                if (writer != null)
                                {
                                    writer.Flush();
                                    writer.Close();
                                }
                                writer = new StreamWriter(targetFileName);
                                fileList.Add(targetFileName);
                                if (file.HasColumnHeaders)
                                {
                                    writer.WriteLine(columnLine);
                                }
                                increment++;
                                currGroup = fileGroup;
                            }
                            var rowString = "";
                            for (var y = 0; y < file.Columns.Count; y++)
                            {
                                rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                if (y != file.Columns.Count - 1)
                                {
                                    rowString += file.FieldDelimeter;
                                }
                            }
                            if (writer != null) writer.WriteLine(rowString);
                        }
                        break;
                    case SplitMethod.ByParts:
                        var totalParts = Convert.ToInt32(valueA);
                        maxSplits = totalParts;
                        int calculatedMaxRecords;
                        var totalRecordsWritten = 0;
                        recordCount = 0;
                        targetFileName =
                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
                        writer = new StreamWriter(targetFileName);
                        fileList.Add(targetFileName);
                        if (file.HasColumnHeaders)
                        {
                            writer.WriteLine(columnLine);
                        }
                        if (randomize)
                        {
                            file.OpenDatabaseSession();
                            calculatedMaxRecords =
                                Convert.ToInt32(Math.Round((double) file.TotalRecords/totalParts));
                            query.Shuffle();
                            dr = file.GetDataReader(query);
                            while (dr.Read())
                            {
                                var rowString = "";
                                for (var y = 0; y < file.Columns.Count; y++)
                                {
                                    rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
                                    if (y != file.Columns.Count - 1)
                                    {
                                        rowString += file.FieldDelimeter;
                                    }
                                }
                                writer.WriteLine(rowString);
                                totalRecordsWritten++;
                                recordCount++;
                                if (recordCount < calculatedMaxRecords ||
                                    (increment >= Convert.ToInt32(maxSplits)) ||
                                    totalRecordsWritten == file.TotalRecords) continue;
                                recordCount = 0;
                                increment++;
                                targetFileName =
                                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                        increment.ToString()));
                                writer.Flush();
                                writer.Close();
                                writer = new StreamWriter(targetFileName);
                                fileList.Add(targetFileName);
                                if (file.HasColumnHeaders)
                                {
                                    writer.WriteLine(columnLine);
                                }
                            }
                        }
                        else
                        {
                            reader = File.OpenText(file.FullName);
                            if (!file.EvaluatedEntirely)
                            {
                                file.EvaluateEntirely();
                            }
                            calculatedMaxRecords = Convert.ToInt32(Math.Round((double) file.TotalRecords/totalParts));
                            if (file.HasColumnHeaders)
                            {
                                reader.ReadLine(); //Skip Columns
                            }
                            line = reader.ReadLine();
                            while (line != null)
                            {
                                writer.WriteLine(line);
                                line = reader.ReadLine();
                                recordCount++;
                                totalRecordsWritten++;
                                if (recordCount < calculatedMaxRecords ||
                                    (increment >= Convert.ToInt32(maxSplits)) ||
                                    totalRecordsWritten == file.TotalRecords) continue;
                                recordCount = 0;
                                increment++;
                                targetFileName =
                                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
                                        increment.ToString()));
                                writer.Flush();
                                writer.Close();
                                writer = new StreamWriter(targetFileName);
                                fileList.Add(targetFileName);
                                if (file.HasColumnHeaders)
                                {
                                    writer.WriteLine(columnLine);
                                }
                            }
                        }
                        break;
                }
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
                if (writer != null)
                {
                    writer.Flush();
                    writer.Close();
                }
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                }
            }
            var rtnList = new List<FileInfo>();
            foreach (var path in fileList)
            {
                rtnList.Add(new FileInfo(path));
            }
            return rtnList;
        }

        protected string GetFileColumnString()
        {
            var columnLine = "";
            if (!IsFixedWidth)
            {
                for (var c = 0; c < Columns.Count; c++)
                {
                    var column = Columns[c];
                    columnLine += ModifyValueBasedOnSettings(column.Name);
                    if (c != Columns.Count - 1)
                    {
                        columnLine += FieldDelimeter;
                    }
                }
            }
            else
            {
                foreach (var column in Columns)
                {
                    var name = ModifyValueBasedOnSettings(column.Name);
                    name = ModifyValueBasedOnColumnLength(column, name);
                    columnLine += name;
                }
            }
            return columnLine;
        }

        protected string GetFileColumnString(Format format, IEnumerable<Column> columns)
        {
            var columnList = columns.ToList();
            var delimeter = "";
            switch (format)
            {
                case Format.CommaDelimited:
                    delimeter = ",";
                    break;
                case Format.TabDelimited:
                    delimeter = "\t";
                    break;
                case Format.PipeDelimited:
                    delimeter = "|";
                    break;
                case Format.DatabaseImport:
                    delimeter = ImportFieldDelimeter;
                    break;
            }
            var columnLine = "";
            if (format != Format.SpaceDelimited)
            {
                for (var c = 0; c < columnList.Count; c++)
                {
                    var column = columnList[c];
                    columnLine += ModifyValueBasedOnSettings(column.Name);
                    if (c != Columns.Count - 1)
                    {
                        columnLine += delimeter;
                    }
                }
            }
            else
            {
                foreach (var column in columnList)
                {
                    var name = ModifyValueBasedOnSettings(column.Name);
                    name = ModifyValueBasedOnColumnLength(column, name);
                    columnLine += name;
                }
            }
            return columnLine;
        }

        protected string ModifyValueBasedOnColumnLength(Column column, string value)
        {
            var variance = column.Length - value.Length;
            if (variance != 0)
            {
                value = variance < 0 ? value.Substring(0, column.Length) : value.PadRight(column.Length);
            }
            return value;
        }

        protected string Substr(string str, int start, int length)
        {
            var variance = str.Length - (start + length);
            var value = variance < 0 ? str.Substring(start) : str.Substring(start, length);
            return value;
        }

        protected static string NormalizePath(string path)
        {
            return Path.GetFullPath(new Uri(path).LocalPath)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .ToUpperInvariant();
        }

        protected static bool IsSamePath(string path1, string path2)
        {
            return NormalizePath(path1) == NormalizePath(path2);
        }

        protected static string GetNextAvailableName(string filePath)
        {
            var targetFile = new FileInfo(filePath);
            var nameWoExt = targetFile.Name.Replace(targetFile.Extension, "");
            var ext = targetFile.Extension;
            var fileNameIncrement = 1;
            while (targetFile.Exists)
            {
                targetFile =
                    new FileInfo(targetFile.DirectoryName + @"\" + nameWoExt + "(" + fileNameIncrement + ")" + ext);
                fileNameIncrement++;
            }
            return targetFile.FullName;
        }

        private void ColumnList_CollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            RefactorColumnNames();
            if (!DatabaseSessionActive) return;
            if (e.OldItems.Count > 0)
            {
                //RemoveColumnsInSql((IEnumerable<Column>) e.OldItems);
            }
            if (e.NewItems.Count > 0)
            {
                //AddColumnsInSql((IEnumerable<Column>) e.OldItems);
            }
        }

        private void RefactorColumnNames()
        {
            const int maxAmtChars = 128;
            foreach (var column in Columns)
            {
                var colName = column.Name.Trim();
                if (string.IsNullOrEmpty(colName))
                {
                    colName = DefaultColumnName;
                }
                var forbiddenChars = new[]
                {"\"", ".", "/", "\\", "[", "]", ":", "|", "<", ">", "+", "=", ";", "*", "?", "-", "\"", "{", "}", "%", ",", "^", "!", "$", "~", ">", "<"};

                colName = forbiddenChars.Aggregate(colName, (current, c) => current.Replace(c, ""));
                var firstChar = Convert.ToChar(colName.Substring(0, 1));
                if (Char.IsDigit(firstChar)) colName = "N_" + colName;

                if (colName.Length > maxAmtChars)
                {
                    colName = colName.Substring(0, maxAmtChars);
                }
                column.Name = colName;
            }

            var lcNames = Columns.Select(s => s.Name.ToLower().Trim()).ToList();
            foreach (var t in Columns)
            {
                var colName = t.Name.Trim();
                var fieldsWithSameName = lcNames.FindAll(s => s.Equals(colName.ToLower()));
                var unique = fieldsWithSameName.Count <= 1;
                if (unique) continue;
                var start = 0;
                for (var x = 1; x < fieldsWithSameName.Count; x++)
                {
                    var name = fieldsWithSameName[x].ToLower().Trim();
                    var index = lcNames.IndexOf(name, start);
                    if (index != -1)
                    {
                        index = lcNames.IndexOf(name, index + 1); //Leave first one alone...
                        if (index != -1)
                        {
                            var numIndicator = "_" + (x + 1);
                            if (numIndicator.Length + colName.Length > maxAmtChars)
                            {
                                var takeOut = maxAmtChars - numIndicator.Length;
                                Columns[index].Name = colName.Substring(0, takeOut) + numIndicator;
                            }
                            else
                            {
                                Columns[index].Name = colName + numIndicator;
                            }
                        }
                    }
                    start = index + 1;
                }
            }
            //Refresh References...
            if (Columns.Count <= 0) return;
            for (var x = 0; x < AggregatableColumns.Count; x++)
            {
                if (AggregatableColumns[x].Index != -1)
                {
                    AggregatableColumns[x] = Columns[AggregatableColumns[x].Index];
                }
            }
        }

        private static string ConvertKeywordToPhrase(string str)
        {
            return Regex.Replace(str, "(\\B[A-Z])", " $1");
        }

        private static string GetFileSize(double bytes)
        {
            const int byteConversion = 1024;

            if (bytes >= Math.Pow(byteConversion, 3)) //GB Range
            {
                return String.Concat(Math.Round(bytes / Math.Pow(byteConversion, 3), 2), " GB");
            }
            if (bytes >= Math.Pow(byteConversion, 2)) //MB Range
            {
                return String.Concat(Math.Round(bytes / Math.Pow(byteConversion, 2), 2), " MB");
            }
            if (bytes >= byteConversion) //KB Range
            {
                return String.Concat(Math.Round(bytes / byteConversion, 2), " KB");
            }
            //Bytes
            return String.Concat(bytes, " Bytes");
        }
    }
}