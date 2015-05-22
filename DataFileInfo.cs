﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using DataFile.Query;
using Excel;

namespace DataFile
{
    public class DataFileInfo
    {
        #region DataFile

        public delegate void OnInitializeHandler();

        public delegate void OnSqlSessionCloseHandler();

        public delegate void OnSqlSessionOpenHandler();

        private const int NumberOfExampleRows = 15;
        private const int NumberOfExampleRowsIfFixedWidth = 5;
        private const string DefaultColumnName = "Column";
        public SqlConnectionStringBuilder ConnectionStringBuilder;
        public int TotalRecords;
        private string _connectionString;
        private Layout _layout;

        public DataFileInfo()
        {
            UniqueIdentifier = Guid.NewGuid().ToString("N");
            HasColumnHeaders = true;
            AggregatableColumns = new ColumnList();
            SampleRows = new List<List<string>>();
            Validity = new Validity();
            InitializeColumnList();
        }

        public DataFileInfo(string filePath, string connectionString = null) : this()
        {
            _connectionString = connectionString;
            Initialize(filePath, true);
        }

        public DataFileInfo(string filePath, bool fileHasColumns, string connectionString = null) : this()
        {
            _connectionString = connectionString;
            Initialize(filePath, fileHasColumns);
        }

        public DataFileInfo(string source, Layout layout, string connectionString = null) : this()
        {
            _connectionString = connectionString;
            Layout = layout;
            Initialize(source, Layout != null && Layout.HasColumnHeaders);
        }

        public string UniqueIdentifier { get; private set; }

        public ColumnList AggregatableColumns { get; set; }
        public ColumnList Columns { get; set; }
        public string DirectoryName { get; set; }
        public bool EvaluatedEntirely { get; set; }
        public List<string> ExcelSheets { get; set; }
        public string Extension { get; set; }
        public string FieldDelimeter { get; set; }
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
        public string Size { get; set; }
        public Validity Validity { get; set; }
        public int Width { get; set; }

        public string ConnectionString
        {
            get { return _connectionString; }
            set
            {
                _connectionString = value;
                ConnectionStringBuilder = new SqlConnectionStringBuilder(value);
            }
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

        public bool LeaveSessionOpenOnDispose { get; set; }

        public event OnInitializeHandler OnInitialize;
        public event OnSqlSessionOpenHandler OnSqlSessionOpen;
        public event OnSqlSessionCloseHandler OnSqlSessionClose;

        private void InitializeColumnList()
        {
            Columns = new ColumnList(ColumnList_CollectionChanged);
        }

        ~DataFileInfo()
        {
            if (!LeaveSessionOpenOnDispose)
            {
                Close();
            }
        }

        public void Close()
        {
            if (!SqlSessionActive) return;
            EndSqlSession();
            if (OnSqlSessionClose != null)
            {
                OnSqlSessionClose();
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

        public void InitializeDelimitedFile()
        {
            StreamReader reader = null;
            try
            {
                reader = File.OpenText(FullName);
                var firstLine = reader.ReadLine();
                if (firstLine != null)
                {
                    if (Extension.ToLower() == ".csv") // trust the extension
                    {
                        FieldDelimeter = ",";
                        SetFormatBasedOnDelimeter();
                    }
                    else
                    {
                        var delimeters = new[] { "\t", ",", "|", SpecialFieldDelimeter };
                        foreach (var delimeter in delimeters)
                        {
                            if (!firstLine.Contains(delimeter)) continue;
                            FieldDelimeter = delimeter;
                            SetFormatBasedOnDelimeter();
                            break;
                        }
                    }
                    if (FieldDelimeter.Length > 0)
                    {
                        var j = 0;
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
                            j++;
                        }
                        while (j < NumberOfExampleRows)
                        {
                            var row = reader.ReadLine();
                            if (row == null)
                            {
                                break;
                            }
                            var fieldItems = SplitByFormat(row).ToList();
                            for (var x = 0; x < fieldItems.Count; x++)
                            {
                                fieldItems[x] = fieldItems[x].Trim();
                            }
                            SampleRows.Add(fieldItems);
                            j++;
                        }
                    }
                    else
                    {
                        var j = 0;
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
                            j++;
                        }
                        Format = Format.SpaceDelimited;
                        while (j < NumberOfExampleRowsIfFixedWidth)
                        {
                            var row = reader.ReadLine();
                            if (row == null)
                            {
                                break;
                            }
                            SampleRows.Add(new List<string> { TrimQuoteDelimeters(row) });
                            j++;
                        }
                    }
                    reader.Close();
                }
            }
            finally
            {
                if (reader != null) reader.Close();
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

        private void Initialize(string filePath, bool fileHasColumns)
        {
            HasColumnHeaders = fileHasColumns;
            var sourceFile = new FileInfo(filePath);
            Length = sourceFile.Length;
            SetFileSize();
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

       

        private void SetFormatBasedOnDelimeter()
        {
            switch (FieldDelimeter)
            {
                case "\t":
                    Format = Format.TabDelimited;
                    break;
                case ",":
                    Format = Format.CommaDelimited;
                    break;
                case "|":
                    Format = Format.PipeDelimited;
                    break;
                default:
                    if (FieldDelimeter == SpecialFieldDelimeter)
                    {
                        Format = Format.FileSessionImport;
                    }
                    break;
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
                                    //var value = TrimQuoteDelimeters(fields[column.Index]);
                                    var value = fields[column.Index];
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

        public void GetFrequenciesForAggregatableColumns()
        {
            foreach (var aggrCol in AggregatableColumns)
            {
                aggrCol.FrequencyValues = GetColumnValueFrequency(aggrCol);
            }
        }

        public List<ColumnValueFrequency> GetColumnValueFrequency(Column column, QueryBuilder query = null)
        {
            if (query == null)
            {
                query = new QueryBuilder(SqlFlavor.TransactSql);
            }
            StartSqlSession();
            string sqlName;
            string alias;
            if (string.IsNullOrEmpty(column.Alias))
            {
                sqlName = BracketWrap(column.Name);
                alias = sqlName;
            }
            else
            {
                sqlName = column.Name;
                alias = BracketWrap(column.Alias);
            }
            query.Select(sqlName + " AS " + alias + ", COUNT(*) as [Count]")
                .OrderBy("COUNT(*) DESC");
            var dtResults = GetDataTable(query);
            var frequencyList = new List<ColumnValueFrequency>();
            foreach (DataRow row in dtResults.Rows)
            {
                var value = row[0];
                var count = Convert.ToInt32(row[1]);
                var percentage = (double) count/TotalRecords;
                var freq = new ColumnValueFrequency(value.ToString(), count, percentage);
                frequencyList.Add(freq);
            }
            if (column.Index >= 0)
            {
                Columns[column.Index].FrequencyValues = frequencyList;
            }
            return frequencyList;
        }

        protected string ModifyValueBasedOnSettings(string value)
        {
            var forceRemovalOfQuotes = false;
            switch (Format)
            {
                case Format.SpaceDelimited:
                case Format.FileSessionImport:
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

        public static string[] ParseCsvRow(string r)
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

        protected static bool LineIsEmpty(string line, string delimeter)
        {
            return string.IsNullOrEmpty(line) || line.Replace(delimeter, "").Trim().Length == 0;
        }

        public static Format GetFileFormatByDelimeter(string delimeter)
        {
            switch (delimeter)
            {
                case ",":
                    return Format.CommaDelimited;
                case "\t":
                    return Format.TabDelimited;
                case "":
                    return Format.SpaceDelimited;
                case "|":
                    return Format.PipeDelimited;
            }
            return Format.Unknown;
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
                case Format.FileSessionImport:
                    newDelimeter = SpecialFieldDelimeter;
                    forceRemovalOfQuotes = true;
                    newExtension = SqlImportFileExtension;
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
                SetFileSize();
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

        protected void UpdatePhysicalFileFromSql()
        {
            QueryToFile(FullName, HasColumnHeaders);
        }

        public DataFileInfo QueryToFile(QueryBuilder query = null, string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, HasColumnHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, QueryBuilder query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, QueryBuilder query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(targetFilePath, withHeaders, null, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, IEnumerable<Column> columns, QueryBuilder query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, columns, query, newDelimeter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, IEnumerable<Column> columns, QueryBuilder query = null,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            var exportIsFixedWidth = newDelimeter != null && newDelimeter == string.Empty || IsFixedWidth;
            var overwritingCurrentFile = string.IsNullOrWhiteSpace(targetFilePath) || IsSamePath(FullName, targetFilePath);
            StartSqlSession();
            SqlQueryToFile(GetSqlColumnString(columns ?? Columns, exportIsFixedWidth), query, targetFilePath,
                newDelimeter, grouplessRecordsOnly, groupId);

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

        public void ImportIntoTable(string targetConnectionString, string targetTable, ColumnList columns = null, QueryBuilder query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            StartSqlSession();
            if (query == null)
            {
                query = new QueryBuilder(SqlFlavor.TransactSql);
            }
            query.Select(GetSqlColumnString(columns ?? Columns));
            SqlQueryToTable(targetConnectionString, targetTable, query, grouplessRecordsOnly, groupId);
        }

        public int UpdateRecords(QueryBuilder query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            StartSqlSession();
            var recordsUpdated = SqlUpdate(groupId, query, grouplessRecordsOnly);
            UpdatePhysicalFileFromSql();
            return recordsUpdated;
        }

        public int DeleteRecords(QueryBuilder query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            StartSqlSession();
            var recordsDeleted = SqlDelete(groupId, query, grouplessRecordsOnly);
            UpdatePhysicalFileFromSql();
            return recordsDeleted;
        }

        public void Shuffle()
        {
            var query = new QueryBuilder(SqlFlavor.TransactSql);
            query.OrderBy("NEWID()");
            QueryToFile(query);
        }

        public void CopyPropertiesFromOtherDataFile(DataFileInfo source)
        {
            HasColumnHeaders = source.HasColumnHeaders;
            AggregatableColumns = source.AggregatableColumns;
            Layout = source.Layout;
        }

        public List<FileSummary> SplitByParts(int numberOfParts, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, newDirectory);
        }

        public List<FileSummary> SplitByParts(int numberOfParts, bool randomize)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, null);
        }

        public List<FileSummary> SplitByParts(int numberOfParts, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByParts, numberOfParts, null, randomize, maxSplits, newDirectory);
        }

        public List<FileSummary> SplitByMaxRecords(int maxRecords, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, newDirectory);
        }

        public List<FileSummary> SplitByMaxRecords(int maxRecords, bool randomize)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, null);
        }

        public List<FileSummary> SplitByMaxRecords(int maxRecords, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, maxSplits, newDirectory);
        }

        public List<FileSummary> SplitByPercentageOfField(int percentage, string fieldName, bool randomize,
            string newDirectory)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, newDirectory);
        }

        public List<FileSummary> SplitByPercentageOfField(int percentage, string fieldName, bool randomize)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, null);
        }

        public List<FileSummary> SplitByPercentageOfField(int percentage, string fieldName, int maxSplits = 0,
            bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, maxSplits,
                newDirectory);
        }

        public List<FileSummary> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                null, newDirectory);
        }

        public List<FileSummary> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            bool randomize)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                null, null);
        }

        public List<FileSummary> SplitByField(ColumnList columnsWithAliases, ColumnList columnsWithoutAliases,
            int maxSplits = 0, bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
                maxSplits, newDirectory);
        }

        public List<FileSummary> SplitByFileQuery(QueryBuilder query, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                null, newDirectory);
        }

        public List<FileSummary> SplitByFileQuery(QueryBuilder query, bool randomize)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                null, null);
        }

        public List<FileSummary> SplitByFileQuery(QueryBuilder query, int maxSplits = 0, bool randomize = false, string newDirectory = null)
        {
            return Split(SplitMethod.ByFileQuery, query, null, randomize,
                maxSplits, newDirectory);
        }

        public List<FileSummary> SplitByFileSize(long maxBytes, bool randomize, string newDirectory)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, newDirectory);
        }

        public List<FileSummary> SplitByFileSize(long maxBytes, bool randomize)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, null);
        }

        public List<FileSummary> SplitByFileSize(long maxBytes, int maxSplits = 0, bool randomize = false,
            string newDirectory = null)
        {
            return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, maxSplits, newDirectory);
        }

        public List<List<FileSummary>> Split(List<SplitOption> options)
        {
            var rtnList = new List<List<FileSummary>>();

            for (var x = 0; x < options.Count; x++)
            {
                var splitOption = options[x];
                var exportDirectoryPath = DirectoryName + "Level_" + (x + 1) + "_" + splitOption.Method;
                var filesToSplit = rtnList.Count != 0
                    ? rtnList[rtnList.Count - 1]
                    : new List<FileSummary> {new FileSummary(FullName)};

                var levelSplits = new List<FileSummary>();
                foreach (var file in filesToSplit)
                {
                    levelSplits.AddRange(Split(splitOption.Method, splitOption.PrimaryValue,
                        splitOption.SecondaryValue, splitOption.Randomize, splitOption.MaxSplits,
                        exportDirectoryPath, file.FullName));
                }
                foreach (var f in levelSplits)
                {
                    f.Attributes["SplitLevel"] = (x + 1);
                }
                rtnList.Add(levelSplits);
            }
            rtnList.Reverse();
            return rtnList;
        }

        protected List<FileSummary> Split(SplitMethod splitBy, object valueA, object valueB,
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
            var query = new QueryBuilder(SqlFlavor.TransactSql);
            var fileList = new List<string>();
            const string partSuffix = "_Part";
            const string incrementPlaceHolder = "[increment]";
            const string countColumnName = "____COUNT";
            const string randomizer = "NEWID()";
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
                        file.StartSqlSession();
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
                                var value = SqlClean(row[x].ToString());
                                where += column.ColumnName + " = '" + value + "'";
                                if (x != counts.Columns.Count - 1)
                                {
                                    where += " AND ";
                                }
                            }

                            query.Select(file.GetSqlColumnString()).Where(where);
                            if (randomize)
                            {
                                query.OrderBy(randomizer);
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
                        file.StartSqlSession();
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
                        query.Select(file.GetSqlColumnString()).Where(whereClause);
                        if (randomize)
                        {
                            query.OrderBy(randomizer);
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

                        query.Select(file.GetSqlColumnString());
                        if (randomize)
                        {
                            query.OrderBy(randomizer);
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
                            file.StartSqlSession();
                            query.Select(file.GetSqlColumnString()).OrderBy(randomizer);
                            
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
                            file.StartSqlSession();
                            query.Select(file.GetSqlColumnString()).OrderBy(randomizer);
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
                        file.StartSqlSession();
                        var percentage = Convert.ToDouble(valueA);
                        var partitionField = valueB.ToString();
                        var fileSplits = Convert.ToInt32(Math.Floor(100/percentage)).ToString();
                        sqlColString = file.GetSqlColumnString() + ",NTILE(" + fileSplits + ") OVER(PARTITION BY " +
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
                            file.StartSqlSession();
                            calculatedMaxRecords =
                                Convert.ToInt32(Math.Round((double) file.TotalRecords/totalParts));
                            query.Select(file.GetSqlColumnString()).OrderBy(randomizer);
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
            var rtnList = new List<FileSummary>();
            foreach (var path in fileList)
            {
                rtnList.Add(new FileSummary(path));
            }
            return rtnList;
        }

        protected static string SqlClean(string s)
        {
            return s.Replace("'", "''");
        }

        public void Delete()
        {
            File.Delete(FullName);
        }

        protected string GetSqlColumnString()
        {
            return GetSqlColumnString(Columns, IsFixedWidth);
        }

        protected string GetSqlColumnString(IEnumerable<Column> columns, bool withAlias = true)
        {
            return GetSqlColumnString(columns, IsFixedWidth, withAlias);
        }

        protected static string GetSqlColumnString(IEnumerable<Column> columns, bool fixedWidth, bool withAlias)
        {
            var formattedColumns = !fixedWidth
                ? columns.Select(column => (BracketWrap(column.Name.Trim()) +
                                            (!withAlias || string.IsNullOrWhiteSpace(column.Alias)
                                                ? ""
                                                : " AS " + BracketWrap(column.Alias)
                                                )
                    ))
                : columns.Select(column => (BracketWrap(column.Name) +
                                            (!withAlias || string.IsNullOrWhiteSpace(column.Alias)
                                                ? ""
                                                : " AS " + BracketWrap(column.Alias)
                                                )
                    ));
            return string.Join(",", formattedColumns);
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

        protected string GetFileColumnString(Format format, ICollection<Column> columns)
        {
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
                case Format.FileSessionImport:
                    delimeter = SpecialFieldDelimeter;
                    break;
            }
            var columnLine = "";
            if (format != Format.SpaceDelimited)
            {
                for (var c = 0; c < columns.Count; c++)
                {
                    var column = Columns[c];
                    columnLine += ModifyValueBasedOnSettings(column.Name);
                    if (c != Columns.Count - 1)
                    {
                        columnLine += delimeter;
                    }
                }
            }
            else
            {
                foreach (var column in columns)
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

        public static string ReplaceWhiteSpaceWithNbsp(string value)
        {
            return Regex.Replace(value, @"\s", "&nbsp;");
        }

        public static string GetNextAvailableName(string filePath)
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

        private void SetFileSize()
        {
            Size = GetFileSize(Length);
        }

        private void ColumnList_CollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {
            RefactorColumnNames();
            if (!SqlSessionActive) return;
            if (e.OldItems.Count > 0)
            {
                RemoveColumnsInSql((IEnumerable<Column>) e.OldItems);
            }
            if (e.NewItems.Count > 0)
            {
                AddColumnsInSql((IEnumerable<Column>) e.OldItems);
            }
        }

        private void AddColumnsInSql(IEnumerable<Column> columns)
        {
            if (SqlSessionActive) return;
            UpdateSchema("DROP COLUMN " + GetColumnDeclarationStatement(columns));
        }

        private void RemoveColumnsInSql(IEnumerable<Column> columns)
        {
            if (!SqlSessionActive) return;

            UpdateSchema("ADD COLUMN " + GetSqlColumnString(columns, false));
        }

        private void UpdateSchema(string alterCommand)
        {
            if (!SqlSessionActive) return;
            ModifySqlSchema(alterCommand);
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
                try
                {
                    if (AggregatableColumns[x].Index != -1)
                    {
                        AggregatableColumns[x] = Columns[AggregatableColumns[x].Index];
                    }
                }
                catch
                {
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

        #endregion

        #region SqlSession

        private static FileInfo currentDll = new FileInfo(Assembly.GetExecutingAssembly().Location);
        private static string SqlProcedureFilesPath
        {
            get
            {
                var path = Path.Combine(currentDll.DirectoryName, @"SqlSessionProcedures\SqlServer");
                return path;
            }
        }
        public static string SqlImportFilesDropPath = @"C:\importFiles";
        public static readonly string SpecialFieldDelimeter = "<#fin#>";
        public static readonly string SqlImportFileExtension = ".fsimport";
        public static readonly string SqlRecordIdColumnName = "___RecordId";
        public static readonly string SqlRecordGroupColumnName = "___GroupId";
        public static int SqlCommandTimeout = 36000;
        public string SqlFormatFilePath;
        public bool SqlSessionActive { get; private set; }

        protected void StartSqlSession()
        {
            if (SqlSessionActive) return;
            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                throw new ArgumentException("The SqlConnectionString must be set in order to use this operation");
            }
            if (Format == Format.XLSX || Format == Format.XLS)
            {
                ConvertTo(Format.CommaDelimited);
            }
            ConnectionStringBuilder = new SqlConnectionStringBuilder(ConnectionString);
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlFilePath = Path.Combine(SqlProcedureFilesPath, "ImportFile.sql");
                var sqlText = File.ReadAllText(sqlFilePath);
                var cmd = new SqlCommand(sqlText, cn) {CommandType = CommandType.Text, CommandTimeout = SqlCommandTimeout};
                if (!EvaluatedEntirely)
                {
                    EvaluateEntirely();
                }

                var localImportFilePath = Path.Combine(SqlImportFilesDropPath, UniqueIdentifier + SqlImportFileExtension);

                //Create Temporary Import File
                var importFile = IsFixedWidth ? SaveAs(Format.FileSessionImport, localImportFilePath) : Copy(localImportFilePath);

                cmd.Parameters.AddWithValue("@dbName", ConnectionStringBuilder.InitialCatalog);
                cmd.Parameters.AddWithValue("@sessionTableId", UniqueIdentifier);
                cmd.Parameters.AddWithValue("@fileLocation", importFile.FullName);
                cmd.Parameters.AddWithValue("@columnsInFile", GetColumnDeclarationStatement(Columns));
                cmd.Parameters.AddWithValue("@fileHasColumnsNames", importFile.HasColumnHeaders);
                cmd.Parameters.AddWithValue("@fieldDelimiter", importFile.FieldDelimeter);
                var formatFilePathParameter = new SqlParameter
                {
                    ParameterName = "@formatFilePath",
                    SqlDbType = SqlDbType.VarChar,
                    Size = 260,
                    Value = DBNull.Value
                };
                cmd.Parameters.Add(formatFilePathParameter);
                FileInfo formatFile = null;
                if (importFile.IsFixedWidth)
                {
                    var formatFilePath = Path.Combine(SqlImportFilesDropPath, UniqueIdentifier + ".xml");
                    CreateBcpFormatFile(formatFilePath);
                    formatFile = new FileInfo(formatFilePath);
                    formatFilePathParameter.Value = formatFilePath;
                }
                cn.Open();
                cmd.ExecuteNonQuery();
                importFile.Delete();
                if (formatFile != null)
                {
                    formatFile.Delete();
                }
                SqlSessionActive = true;
                if (OnSqlSessionOpen != null)
                {
                    OnSqlSessionOpen();
                }
            }
            finally
            {
                cn.Close();
                cn.Dispose();
            }
        }

        protected void EndSqlSession()
        {
            if (!SqlSessionActive)
            {
                return;
            }
            SqlSessionActive = false;
        }

        private SqlDataReader GetDataReader(QueryBuilder query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (SqlDataReader) SqlSelect(true, query, grouplessRecordsOnly, groupId);
        }

        private DataTable GetDataTable(QueryBuilder query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (DataTable) SqlSelect(false, query, grouplessRecordsOnly, groupId);
        }

        private object SqlSelect(bool dataReader, QueryBuilder query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                CreateGroupPartition(groupId, grouplessRecordsOnly, query);
                cn.Open();
                var selectQuery = query.Clone();
                if (groupId != null)
                {
                    selectQuery.ClearWhereClause().Where("{0} = '{1}'", SqlRecordGroupColumnName, groupId);
                }
                selectQuery.From(BracketWrap(UniqueIdentifier));
                using (var command = new SqlCommand(selectQuery.ToString(), cn) { CommandType = CommandType.Text, CommandTimeout = SqlCommandTimeout })
                {
                    if (dataReader)
                    {
                        return command.ExecuteReader(CommandBehavior.CloseConnection);
                    }
                    else
                    {
                        var da = new SqlDataAdapter(command);
                        var dt = new DataTable("QueryResults");
                        da.Fill(dt);
                        cn.Close();
                        cn.Dispose();
                        return dt;
                    }
                }
            }
            catch
            {
                cn.Close();
                throw;
            }
            finally
            {
                if (!dataReader)
                {
                    cn.Close();
                }
            }
        }

        private void CreateGroupPartition(string groupId, bool grouplessRecordsOnly = false, QueryBuilder query = null)
        {
            if (string.IsNullOrWhiteSpace(groupId)) return;
            var cn = new SqlConnection(ConnectionString);
            if (query == null)
            {
                query = new QueryBuilder(SqlFlavor.TransactSql);
            }
            try
            {
                var releaseGroupQuery = new QueryBuilder(SqlFlavor.TransactSql)
                    .Update(BracketWrap(UniqueIdentifier))
                    .Set("{0} = NULL", SqlRecordGroupColumnName)
                    .Where("{0} = '{1}'", SqlRecordGroupColumnName, groupId);
                using (var command = new SqlCommand(releaseGroupQuery.ToString(), cn))
                {
                    command.ExecuteNonQuery();
                }

                var assignGroupQuery =  query
                    .Clone()
                    .Update(BracketWrap(UniqueIdentifier))
                    .Set("{0} = '{1}'", SqlRecordGroupColumnName, groupId);

                if (grouplessRecordsOnly)
                {
                    assignGroupQuery.Where("{0} IS NULL", SqlRecordGroupColumnName);
                }

                using (var command = new SqlCommand(assignGroupQuery.ToString(), cn))
                {
                    command.ExecuteNonQuery();
                }
            }
            finally
            {
                cn.Close();
            }
            
        }

        protected DataTable GetSqlSchema(QueryBuilder query)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                cn.Open();
                var schemaTable = new DataTable();
                var schemaQuery = new QueryBuilder(query.Flavor);
                schemaQuery
                    .Select(query.SelectClause)
                    .Limit(1)
                    .From(WrapWithBrackets(UniqueIdentifier))
                    .Where("1 = 2");
                using (var command = new SqlCommand(schemaQuery.ToString(), cn))
                {
                    var da = new SqlDataAdapter(command);
                    da.FillSchema(schemaTable, SchemaType.Source);
                }
                return schemaTable;
            }
            finally
            {
                cn.Close();
            }
        }

        private int SqlUpdate(string groupId, QueryBuilder query, bool grouplessRecordsOnly)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlFilePath = Path.Combine(SqlProcedureFilesPath, "Update.sql");
                var sqlText = File.ReadAllText(sqlFilePath);
                var cmd = new SqlCommand(sqlText, cn) {CommandType = CommandType.Text, CommandTimeout = SqlCommandTimeout};
                cmd.Parameters.AddWithValue("@dbName", ConnectionStringBuilder.InitialCatalog);
                cmd.Parameters.AddWithValue("@sessionTableId", UniqueIdentifier);
                cmd.Parameters.AddWithValue("@groupId", groupId);
                cmd.Parameters.AddWithValue("@updateClause", query.SetClause);
                cmd.Parameters.AddWithValue("@whereClause", query.WhereClause);
                cmd.Parameters.AddWithValue("@grouplessRecordsOnly", grouplessRecordsOnly);

                cn.Open();
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        private void ModifySqlSchema(string alterSchemaClause)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlFilePath = Path.Combine(SqlProcedureFilesPath, "AlterTable.sql");
                var sqlText = File.ReadAllText(sqlFilePath);
                var cmd = new SqlCommand(sqlText, cn) {CommandType = CommandType.Text, CommandTimeout = SqlCommandTimeout};
                cmd.Parameters.AddWithValue("@dbName", ConnectionStringBuilder.InitialCatalog);
                cmd.Parameters.AddWithValue("@sessionTableId", UniqueIdentifier);
                cmd.Parameters.AddWithValue("@alterSchemaClause", alterSchemaClause);
                cn.Open();
                cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        private int SqlDelete(string groupId, QueryBuilder query, bool grouplessRecordsOnly)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlFilePath = Path.Combine(SqlProcedureFilesPath, "Delete.sql");
                var sqlText = File.ReadAllText(sqlFilePath);
                var cmd = new SqlCommand(sqlText, cn) {CommandType = CommandType.Text, CommandTimeout = SqlCommandTimeout};
                cmd.Parameters.AddWithValue("@dbName", ConnectionStringBuilder.InitialCatalog);
                cmd.Parameters.AddWithValue("@sessionTableId", UniqueIdentifier);
                cmd.Parameters.AddWithValue("@groupId", groupId);
                cmd.Parameters.AddWithValue("@whereClause", query.WhereClause);
                cmd.Parameters.AddWithValue("@grouplessRecordsOnly", grouplessRecordsOnly);

                cn.Open();
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        private void SqlQueryToFile(string columns, QueryBuilder query, string targetFilePath,
            string newDelimeter, bool grouplessRecordsOnly, string groupId)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var targetFile = new FileInfo(targetFilePath);
                var delimeter = newDelimeter ?? FieldDelimeter;
                var sqlFilePath = Path.Combine(SqlProcedureFilesPath, "QueryToFile.sql");
                var sqlText = File.ReadAllText(sqlFilePath);
                var cmd = new SqlCommand(sqlText, cn) {CommandType = CommandType.Text, CommandTimeout = 0};
                cmd.CommandTimeout = SqlCommandTimeout;
                cmd.Parameters.AddWithValue("@dbName", ConnectionStringBuilder.InitialCatalog);
                cmd.Parameters.AddWithValue("@sessionTableId", UniqueIdentifier);
                cmd.Parameters.AddWithValue("@groupId", groupId);
                cmd.Parameters.AddWithValue("@columns", columns);
                cmd.Parameters.AddWithValue("@whereClause", query.WhereClause);
                cmd.Parameters.AddWithValue("@groupByClause", query.GroupByClause);
                cmd.Parameters.AddWithValue("@havingClause", query.HavingClause);
                cmd.Parameters.AddWithValue("@orderByClause", query.OrderByClause);
                cmd.Parameters.AddWithValue("@targetFilePath", targetFilePath);
                cmd.Parameters.AddWithValue("@fieldDelimiter", delimeter);
                cmd.Parameters.AddWithValue("@grouplessRecordsOnly", grouplessRecordsOnly);
                var numberOfRecordsCopied = new SqlParameter
                {
                    Direction = ParameterDirection.Output,
                    DbType = DbType.Int32,
                    ParameterName = "@numberOfRecordsCopied",
                    Value = 0
                };
                cmd.Parameters.Add(numberOfRecordsCopied);

                if (targetFile.Exists)
                {
                    targetFile.Delete();
                }
                cn.Open();
                cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
                cn.Dispose();
            }
        }

        private void SqlQueryToTable(string targetConnectionString, string targetTable, QueryBuilder query, bool grouplessRecordsOnly, string groupId = null)
        {
            var sourceTableConnection = new SqlConnection(ConnectionString);
            var targetTableConnection = new SqlConnection(targetConnectionString ?? ConnectionString);
            try
            {
                targetTableConnection.Open();
                bool tableExists;
                const string sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = @TableId";
                using (var command = new SqlCommand(sql, targetTableConnection))
                {
                    command.Parameters.AddWithValue("@TableId", targetTable);
                    var count = Convert.ToInt32(command.ExecuteScalar());
                    tableExists = count > 0;
                }

                if (!tableExists)
                {
                    sourceTableConnection.Open();
                    var sourceTableSchema = new DataTable();
                    var schemaQuery = new QueryBuilder(query.Flavor);
                    schemaQuery
                        .Select(query.SelectClause)
                        .Limit(1)
                        .From(WrapWithBrackets(UniqueIdentifier))
                        .Where("1 = 2");
                    using (var command = new SqlCommand(schemaQuery.ToString(), sourceTableConnection))
                    {
                        var da = new SqlDataAdapter(command);
                        da.FillSchema(sourceTableSchema, SchemaType.Source);
                    }

                    var tableCreator = new TransactSqlTableCreator(targetTableConnection) { DestinationTableName = targetTable };
                    tableCreator.CreateFromDataTable(sourceTableSchema);
                }

                using (var bulkCopy = new SqlBulkCopy(targetTableConnection))
                {
                    bulkCopy.DestinationTableName = "[" + targetTable + "]";
                    bulkCopy.WriteToServer(GetDataReader(query,grouplessRecordsOnly,groupId));
                }
            }
            finally
            {
                sourceTableConnection.Close();
                targetTableConnection.Close();
            }
        }

        private static string GetColumnDeclarationStatement(IEnumerable<Column> columns)
        {
            var columnsCreateStatement = new List<string>();
            foreach (var column in columns)
            {
                var length = column.Length > 0 ? column.Length : 1;
                columnsCreateStatement.Add(WrapWithBrackets(column.Name) + " CHAR(" + length + ")");
            }
            return string.Join(",", columnsCreateStatement);
        }

        private XmlDocument CreateBcpFormatFile()
        {
            const string xsiUri = "http://www.w3.org/2001/XMLSchema-instance";
            var ff = new XmlDocument();
            var dec = ff.CreateXmlDeclaration("1.0", null, null);
            ff.AppendChild(dec);
            var bcpFormat = ff.CreateElement("BCPFORMAT");
            bcpFormat.SetAttribute("xmlns", "http://schemas.microsoft.com/sqlserver/2004/bulkload/format");
            bcpFormat.SetAttribute("xmlns:xsi", xsiUri);
            var record = ff.CreateElement("RECORD");
            var row = ff.CreateElement("ROW");
            for (var x = 0; x < Columns.Count; x++)
            {
                var col = Columns[x];
                var id = (col.Index + 1).ToString();
                var length = col.Length.ToString();
                var column = ff.CreateElement("COLUMN");
                column.SetAttribute("SOURCE", id);
                column.SetAttribute("NAME", col.Name);
                column.SetAttribute("type", xsiUri, "SQLCHAR");
                column.SetAttribute("LENGTH", length);


                var field = ff.CreateElement("FIELD");
                field.SetAttribute("ID", id);
                if (x != Columns.Count - 1)
                {
                    field.SetAttribute("type", xsiUri, "CharFixed");
                    field.SetAttribute("LENGTH", length);
                }
                else
                {
                    field.SetAttribute("type", xsiUri, "CharTerm");
                    field.SetAttribute("TERMINATOR", @"\r\n");
                }

                record.AppendChild(field);
                row.AppendChild(column);
            }
            bcpFormat.AppendChild(record);
            bcpFormat.AppendChild(row);
            ff.AppendChild(bcpFormat);
            return ff;
        }

        private void CreateBcpFormatFile(string saveToPath)
        {
            var xml = CreateBcpFormatFile();
            xml.Save(saveToPath);
        }

        protected static string WrapWithBrackets(string columns)
        {
            if (!columns.Contains(",")) return BracketWrap(columns);
            var split = columns.Split(',');
            for (var i = 0; i < split.Length; i++)
            {
                split[i] = WrapWithBrackets(split[i]);
            }
            return String.Join(",", split);
        }

        protected static string BracketWrap(string columnName)
        {
            if (!columnName.EndsWith("]"))
            {
                columnName = "[" + columnName + "]";
            }
            return columnName;
        }

        #endregion
    }
}