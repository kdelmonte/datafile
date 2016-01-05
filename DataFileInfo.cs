using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using DataFile.Interfaces;
using DataFile.Models;
using DataFile.Models.Database;

namespace DataFile
{
    public partial class DataFileInfo: IDisposable
    {
        
        // Properties
        //=================================================

        public delegate void OnInitializeHandler();

        public delegate void OnDatabaseSessionCloseHandler();

        public delegate void OnDatabaseSessionOpenHandler();

        public static readonly string ImportFieldDelimiter = "<#fin#>";
        public static readonly string DatabaseImportFileExtension = ".dfimport";
        
        public int NumberOfExampleRows { get; set; }
        public string DefaultColumnName { get; set; }
        public int TotalRecords { get; private set; }
        public string TableName { get; set; }

        public string DirectoryName { get; set; }
        public bool Analyzed { get; private set; }
        public List<string> Sheets { get; set; }
        public string Extension { get; set; }

        public string FullName { get; set; }
        

        public bool IsFixedWidth => Layout.Format == DataFileFormat.SpaceDelimited;

        public long Length { get; set; }
        public string Name { get; set; }
        public string NameWithoutExtension { get; set; }
        public List<List<object>> SampleRows { get; set; }

        public DataFileValidity Validity { get; private set; }
        
        public IDataFileDbAdapter DatabaseAdapter { get; set; }

        public bool Exists => File.Exists(FullName);

        public bool IsEmpty => SampleRows.Count == 0;

        public string ActiveWorksheet { get; private set; }

        public DataFileLayout Layout { get; private set; }

        public bool KeepSessionOpenOnDispose { get; set; }
        

        public event OnInitializeHandler OnInitialize;
        public event OnDatabaseSessionOpenHandler OnDatabaseSessionOpen;
        public event OnDatabaseSessionCloseHandler OnDatabaseSessionClose;
        
        // Constructors/Destructors
        //=================================================

        public DataFileInfo()
        {
            DefaultColumnName = "Column";
            NumberOfExampleRows = 15;
            Layout = new DataFileLayout();
            TableName = Guid.NewGuid().ToString("N");
            SampleRows = new List<List<object>>();
            Validity = new DataFileValidity();
        } 

        public DataFileInfo(IDataFileDbAdapter dbAdapter = null): this()
        {
            DatabaseAdapter = dbAdapter;
        }

        public DataFileInfo(string filePath, IDataFileDbAdapter dbAdapter = null)
            : this(dbAdapter)
        {
            FullName = filePath;
            Initialize();
        }

        public DataFileInfo(string filePath, DataFileLayout layout, IDataFileDbAdapter dbAdapter = null)
            : this(dbAdapter)
        {
            FullName = filePath;
            Layout = layout;
            Initialize();
        }

        public DataFileInfo(string filePath, bool fileHasColumns, IDataFileDbAdapter dbAdapter = null)
            : this(filePath, new DataFileLayout { HasColumnHeaders = fileHasColumns }, dbAdapter)
        {
        }

        ~DataFileInfo()
        {
            try
            {
                Dispose();
            }
            catch
            {
                // We can ignore this. If this fails, it is most likely because
                // the instance had already been destroyed by Garbage Collection.
            }
        }

        
        // Public Methods
        //=================================================


        public DataFileReader GetDataReader(bool onColumnRow = false, bool onActiveSheet = true)
        {
            var reader = new DataFileReader(FullName,Layout);
            if (onColumnRow && Layout.HasColumnHeaders)
            {
                reader.StartAt = 1;
            }
            if (onActiveSheet)
            {
                reader.TargetWorksheetName = ActiveWorksheet;
            }
            reader.Open();
            return reader;
        }

        public DataFileWriter GetDataWriter()
        {
            var writer = new DataFileWriter(FullName, Layout);
            writer.Open();
            return writer;
        }
        
        public void Dispose()
        {
            if (!DatabaseSessionActive || KeepSessionOpenOnDispose) return;
            StopDatabaseSession();
            OnDatabaseSessionClose?.Invoke();
        }

        public void SwitchToWorkSheet(int sheetIndex)
        {
            SwitchToWorkSheet(Sheets[sheetIndex]);
        }

        public void SwitchToWorkSheet(string sheetName)
        {
            Layout.Columns.Clear();
            SampleRows.Clear();
            ActiveWorksheet = sheetName;
            Initialize();
        }

        public void AnalyzeFromDisk()
        {
            var totalRecords = 0;
            if (IsFixedWidth && Layout != null)
            {
                Analyzed = true;
                return;
            }
            using (var reader = GetDataReader(true))
            {
                while (reader.Read())
                {
                    for (var x = 0; x < Layout.Columns.Count; x++)
                    {
                        var column = Layout.Columns[x];
                        var value = reader[x].ToString();
                        if (value.Length > column.Length)
                        {
                            column.Length = value.Length;
                        }
                        Layout.Columns[x] = column;
                    }
                    totalRecords++;
                }
            }
            foreach (var column in Layout.Columns.Where(column => column.Length == 0))
            {
                column.Length = 1;
            }
            TotalRecords = totalRecords;
            Analyzed = true;
        }

        public DataFileValidity Validate()
        {
            var validity = new DataFileValidity();
            var unconfirmedErrors = new List<DataFileValueValidity>();
            var currentRow = 0;
            using (var reader = GetDataReader(true))
            {
                while (reader.Read())
                {
                    var rowErrors = new List<DataFileValueValidity>();
                    for (var index = 0; index < Layout.Columns.Count; index++)
                    {
                        var column = Layout.Columns[index];
                        var value = reader[index];
                        var valueValidity = column.ValidateValue(value);
                        if (!valueValidity.Valid)
                        {
                            valueValidity.ColumnIndex = index;
                            valueValidity.RowNumber = currentRow;
                            rowErrors.Add(valueValidity);
                        }
                    }
                    if (reader.CurrentRecord.IsEmpty())
                    {
                        unconfirmedErrors.AddRange(rowErrors);
                    }
                    else
                    {
                        validity.InvalidValues.AddRange(unconfirmedErrors);
                        unconfirmedErrors.Clear();
                        validity.InvalidValues.AddRange(rowErrors);
                    }
                    currentRow++;
                }
            }
            if (validity.HasInvalidValues)
            {
                validity.AddError("File contains invalid values");
            }
            if (currentRow == 0)
            {
                validity.AddWarning("File is empty");
            }
            Validity = validity;
            return Validity;
        }

        public void ChangeLayout(DataFileLayout layout, List<DataFileColumnMapping> mappings = null)
        {
            var saveSettings = new DataFileSaveSettings
            {
                Path = FullName,
                Layout = layout,
                Mappings = mappings,
                Overwrite = true
            };
            Save(saveSettings);
        }

        public void ConvertTo(DataFileLayout layout = null, List<DataFileColumnMapping> mappings = null)
        {
            var saveSettings = new DataFileSaveSettings
            {
                Path = FullName,
                Layout = layout,
                Mappings = mappings,
                Overwrite = true
            };
            Save(saveSettings);
        }

        public DataFileInfo SaveAs(DataFileFormat format, string targetPath, bool overwrite = false, List<DataFileColumnMapping> mappings = null)
        {
            var layout = Layout.Clone();
            layout.Format = format;
            var saveSettings = new DataFileSaveSettings
            {
                Path = targetPath,
                Layout = layout,
                Mappings = mappings,
                Overwrite = overwrite
            };
            Save(saveSettings);
            return new DataFileInfo(targetPath, layout, DatabaseAdapter);
        }

        public DataFileInfo SaveAs(string targetPath, bool overwrite = false, DataFileLayout layout = null, List<DataFileColumnMapping> mappings = null)
        {
            var saveSettings = new DataFileSaveSettings
            {
                Path = targetPath,
                Layout = layout,
                Mappings = mappings,
                Overwrite = overwrite
            };
            Save(saveSettings);
            return new DataFileInfo(targetPath, layout, DatabaseAdapter);
        }

        public DataFileInfo Copy(string targetPath, bool overwrite = false)
        {
            File.Copy(FullName, targetPath, overwrite);
            var newLfi = new DataFileInfo(targetPath, Layout);
            return newLfi;
        }

        public DataFileInfo QueryToFile(DataFileQuery query = null, string newFieldDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, Layout.HasColumnHeaders, null, query, newFieldDelimiter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, DataFileQuery query = null,
            string newFieldDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, null, query, newFieldDelimiter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, DataFileQuery query = null,
            string newFieldDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(targetFilePath, withHeaders, null, query, newFieldDelimiter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(bool withHeaders, IEnumerable<DataFileColumn> columns, DataFileQuery query = null,
            string newFieldDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return QueryToFile(null, withHeaders, columns, query, newFieldDelimiter, grouplessRecordsOnly, groupId);
        }

        public DataFileInfo QueryToFile(string targetFilePath, bool withHeaders, IEnumerable<DataFileColumn> columns, DataFileQuery query = null,
            string newFieldDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            var exportIsFixedWidth = newFieldDelimiter != null && newFieldDelimiter == string.Empty || IsFixedWidth;
            var overwritingCurrentFile = string.IsNullOrWhiteSpace(targetFilePath) || IsSamePath(FullName, targetFilePath);
            QueryToFile(query, targetFilePath, newFieldDelimiter, grouplessRecordsOnly, groupId);
            var dataFileInfo = overwritingCurrentFile ? this : new DataFileInfo(targetFilePath, false);
            if (withHeaders)
            {
                dataFileInfo.InsertColumnHeaders();
            }
            return dataFileInfo;
        }

        public void InsertColumnHeaders()
        {
            if (Layout.HasColumnHeaders) return;
            var tempFilePath = Path.GetTempFileName();
            using (var reader = GetDataReader(true))
            {
                using (var writer = new DataFileWriter(tempFilePath, Layout))
                {
                    while (reader.Read())
                    {
                        writer.Write(reader.GetValues());
                    }

                    writer.Close();
                    reader.Close();
                    File.Copy(tempFilePath, FullName, true);
                    try
                    {
                        File.Delete(tempFilePath);
                    }
                    catch
                    {
                    }
                    Layout.HasColumnHeaders = true;
                }
            }
        }

        public void RemoveColumnHeaders()
        {
            if (!Layout.HasColumnHeaders) return;
            var tempFilePath = Path.GetTempFileName();
            using (var reader = GetDataReader(true))
            {
                using(var writer = new DataFileWriter(tempFilePath, Layout))
                {
                    while (reader.Read())
                    {
                        writer.Write(reader.GetValues());
                    }

                    writer.Close();
                    reader.Close();
                    File.Copy(tempFilePath, FullName, true);
                    try
                    {
                        File.Delete(tempFilePath);
                    }
                    catch
                    {
                    }
                    Layout.HasColumnHeaders = false;
                }
            }
        }

        public void Shuffle()
        {
            var query = CreateQuery();
            query.Shuffle();
            QueryToFile(query);
        }

        

        public void Delete()
        {
            File.Delete(FullName);
        }

        // Private Methods
        //=================================================


        private void DetermineFieldDelimiter()
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
                        Layout.FieldDelimiter = ",";
                    }
                    else
                    {
                        var delimiters = new[] { "\t", ",", "|", ImportFieldDelimiter };
                        foreach (var delimiter in delimiters)
                        {
                            if (!firstLine.Contains(delimiter)) continue;
                            Layout.FieldDelimiter = delimiter;
                            break;
                        }
                    }
                }
            }
            finally
            {
                reader?.Close();
            }
        }

        private void InitializeColumnProperties()
        {
            if (!IsFixedWidth)
            {
                for (var x = 0; x < Layout.Columns.Count; x++)
                {
                    var column = Layout.Columns[x];
                    foreach (var row in SampleRows)
                    {
                        var value = row[x];
                        column.ExampleValue = value;
                    }
                    Layout.Columns[x] = column;
                }
            }
        }

        private void SetFileProperties()
        {
            var sourceFile = new FileInfo(FullName);
            if (!sourceFile.Exists) return;
            Length = sourceFile.Length;
            FullName = sourceFile.FullName;
            Name = sourceFile.Name;
            DirectoryName = sourceFile.DirectoryName + @"\";
            NameWithoutExtension = Path.GetFileNameWithoutExtension(FullName);
            Extension = sourceFile.Extension;
        }

        private void RetrieveSheets()
        {
            Sheets = new List<string>();
            using (var reader = GetDataReader(false,false))
            {
                if (!string.IsNullOrWhiteSpace(reader.CurrentWorksheet))
                {
                    Sheets.Add(reader.CurrentWorksheet);
                }
                while (reader.NextResult())
                {
                    Sheets.Add(reader.CurrentWorksheet);
                }
            }
            
        }

        private void Initialize()
        {
            if (!Exists) return;
            SetFileProperties();
            var extension = Extension.ToLower().Replace(".", "");
            switch (extension)
            {
                case "xls":
                case "xlsx":
                    Layout.Format = extension == "xlsx" ? DataFileFormat.XLSX : DataFileFormat.XLS;
                    RetrieveSheets();
                    break;
                default:
                    DetermineFieldDelimiter();
                    break;
            }

            using (var reader = GetDataReader(false, false))
            {
                if (reader.Read())
                {
                    if (!Layout.Columns.Any())
                    {
                        if (Layout.HasColumnHeaders)
                        {
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                Layout.Columns.Add(new DataFileColumn(reader[i].ToString()));
                            }
                        }
                        else
                        {
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                Layout.Columns.Add(new DataFileColumn(DefaultColumnName + (i + 1)));
                            }
                            SampleRows.Add(reader.GetValues().ToList());
                        }
                    }
                    else
                    {
                        var headersFound = true;
                        for (var i = 0; i < Layout.Columns.Count; i++)
                        {
                            var value = reader[i].ToString();
                            var column = Layout.Columns[i];
                            if (!column.Name.Trim().Equals(value.Trim(), StringComparison.OrdinalIgnoreCase))
                            {
                                headersFound = false;
                                break;
                            }
                        }
                        Layout.HasColumnHeaders = headersFound;

                        if (!Layout.HasColumnHeaders)
                        {
                            SampleRows.Add(reader.GetValues().ToList());
                        }
                    }

                    while (reader.Read() && SampleRows.Count < NumberOfExampleRows)
                    {
                        SampleRows.Add(reader.GetValues().ToList());
                    }
                }
            }
            
            foreach (var column in Layout.Columns.Where(column => column.Length == 0))
            {
                column.Length = 1;
            }
            InitializeColumnProperties();
            OnInitialize?.Invoke();
        }



        private void Save(DataFileSaveSettings settings)
        {
            if (string.IsNullOrWhiteSpace(settings.Path))
            {
                throw new ArgumentException("The specified path is invalid");
            }
            if (settings.Layout == null)
            {
                throw new ArgumentException("A layout must be specified");
            }
            var targetFileIsCurrentFile = Path.GetFullPath(settings.Path.ToLower()).Equals(Path.GetFullPath(FullName.ToLower()));
            if (IsFixedWidth)
            {
                if (Layout == null)
                {
                    throw new Exception("The current Fixed Width File has no layout. Please specify and try again.");
                }
            }
            var eligibleForLayoutChange = settings.Mappings != null && settings.Mappings.Count > 0 && settings.Layout != null;
            switch (settings.Layout.Format)
            {
                case DataFileFormat.SpaceDelimited:
                    if (!eligibleForLayoutChange)
                    {
                        throw new Exception(
                            "A lead Layout and field to field mappings are required to save as space delimited");
                    }
                    break;
            }
            var targetFilePath = settings.Path;
            var targetFile = new FileInfo(settings.Path);
            var overwriting = false;
            if (targetFile.Exists)
            {
                targetFilePath = Path.GetTempFileName();
                overwriting = true;
            }
            using (var reader = GetDataReader(true))
            {
                using (var writer = new DataFileWriter(targetFilePath, settings.Layout))
                {
                    writer.Open();
                    if (settings.Layout.HasColumnHeaders)
                    {
                        writer.Write(settings.Layout.Columns.Select(column => column.Name));
                    }
                    if (!eligibleForLayoutChange)
                    {
                        while (reader.Read())
                        {
                            writer.Write(reader.GetValues());
                        }
                    }
                    else
                    {
                        while (reader.Read())
                        {
                            var row = new List<object>();
                            for (int index = 0; index < Layout.Columns.Count; index++)
                            {
                                var targetColumn = Layout.Columns[index];
                                var foundMap = false;
                                foreach (var mapping in settings.Mappings)
                                {
                                    if (mapping.Target != index) continue;
                                    if (Layout.Columns.Count > mapping.Source)
                                    {
                                        var value = reader[mapping.Source];
                                        row.Add(value);
                                        foundMap = true;
                                    }
                                    break;
                                }
                                if (!foundMap)
                                {
                                    row.Add(string.Empty);
                                }
                            }
                            writer.Write(row);
                        }
                    }
                }
            }

            if (overwriting)
            {
                if (targetFile.Exists)
                {
                    targetFile.Delete();
                }
                File.Move(targetFilePath, settings.Path);
            }
            if (!targetFileIsCurrentFile) return;
            File.Delete(FullName);

            FullName = settings.Path;
            Layout = settings.Layout;
            var fi = new FileInfo(FullName);
            Name = fi.Name;
            Extension = fi.Extension;
            NameWithoutExtension = Path.GetFileNameWithoutExtension(FullName);
            Length = fi.Length;
        }

        private void Pull()
        {
            QueryToFile(FullName, Layout.HasColumnHeaders);
        }

        private string ModifyValueBasedOnColumnLength(DataFileColumn column, string value)
        {
            var variance = column.Length - value.Length;
            if (variance != 0)
            {
                value = variance < 0 ? value.Substring(0, column.Length) : value.PadRight(column.Length);
            }
            return value;
        }

        private string Substr(string str, int start, int length)
        {
            var variance = str.Length - (start + length);
            var value = variance < 0 ? str.Substring(start) : str.Substring(start, length);
            return value;
        }

        private static string NormalizePath(string path)
        {
            return Path.GetFullPath(new Uri(path).LocalPath)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .ToUpperInvariant();
        }

        private static bool IsSamePath(string path1, string path2)
        {
            return NormalizePath(path1) == NormalizePath(path2);
        }

        private static string GetNextAvailableName(string filePath)
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

        private static string ConvertCodeToWords(string str)
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



        // TODO: Rewrite Split Methods
        //public List<FileInfo> SplitByParts(int numberOfParts, bool randomize, string newDirectory)
        //{
        //    return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, newDirectory);
        //}

        //public List<FileInfo> SplitByParts(int numberOfParts, bool randomize)
        //{
        //    return Split(SplitMethod.ByParts, numberOfParts, null, randomize, null, null);
        //}

        //public List<FileInfo> SplitByParts(int numberOfParts, int maxSplits = 0, bool randomize = false,
        //    string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByParts, numberOfParts, null, randomize, maxSplits, newDirectory);
        //}

        //public List<FileInfo> SplitByMaxRecords(int maxRecords, bool randomize, string newDirectory)
        //{
        //    return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, newDirectory);
        //}

        //public List<FileInfo> SplitByMaxRecords(int maxRecords, bool randomize)
        //{
        //    return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, null, null);
        //}

        //public List<FileInfo> SplitByMaxRecords(int maxRecords, int maxSplits = 0, bool randomize = false,
        //    string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByMaxRecords, maxRecords, null, randomize, maxSplits, newDirectory);
        //}

        //public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, bool randomize,
        //    string newDirectory)
        //{
        //    return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, newDirectory);
        //}

        //public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, bool randomize)
        //{
        //    return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, null, null);
        //}

        //public List<FileInfo> SplitByPercentageOfField(int percentage, string fieldName, int maxSplits = 0,
        //    bool randomize = false, string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByPercentage, percentage, fieldName, randomize, maxSplits,
        //        newDirectory);
        //}

        //public List<FileInfo> SplitByField(DataFileColumnList columnsWithAliases, DataFileColumnList columnsWithoutAliases,
        //    bool randomize, string newDirectory)
        //{
        //    return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
        //        null, newDirectory);
        //}

        //public List<FileInfo> SplitByField(DataFileColumnList columnsWithAliases, DataFileColumnList columnsWithoutAliases,
        //    bool randomize)
        //{
        //    return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
        //        null, null);
        //}

        //public List<FileInfo> SplitByField(DataFileColumnList columnsWithAliases, DataFileColumnList columnsWithoutAliases,
        //    int maxSplits = 0, bool randomize = false, string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByField, columnsWithAliases, columnsWithoutAliases, randomize,
        //        maxSplits, newDirectory);
        //}

        //public List<FileInfo> SplitByFileQuery(DataFileQuery query, bool randomize, string newDirectory)
        //{
        //    return Split(SplitMethod.ByFileQuery, query, null, randomize,
        //        null, newDirectory);
        //}

        //public List<FileInfo> SplitByFileQuery(DataFileQuery query, bool randomize)
        //{
        //    return Split(SplitMethod.ByFileQuery, query, null, randomize,
        //        null, null);
        //}

        //public List<FileInfo> SplitByFileQuery(DataFileQuery query, int maxSplits = 0, bool randomize = false, string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByFileQuery, query, null, randomize,
        //        maxSplits, newDirectory);
        //}

        //public List<FileInfo> SplitByFileSize(long maxBytes, bool randomize, string newDirectory)
        //{
        //    return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, newDirectory);
        //}

        //public List<FileInfo> SplitByFileSize(long maxBytes, bool randomize)
        //{
        //    return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, null, null);
        //}

        //public List<FileInfo> SplitByFileSize(long maxBytes, int maxSplits = 0, bool randomize = false,
        //    string newDirectory = null)
        //{
        //    return Split(SplitMethod.ByFileSize, maxBytes, null, randomize, maxSplits, newDirectory);
        //}

        //public List<List<FileInfo>> Split(List<SplitOption> options)
        //{
        //    var rtnList = new List<List<FileInfo>>();

        //    for (var x = 0; x < options.Count; x++)
        //    {
        //        var splitOption = options[x];
        //        var exportDirectoryPath = DirectoryName + "Level_" + (x + 1) + "_" + splitOption.Method;
        //        var filesToSplit = rtnList.Count != 0
        //            ? rtnList[rtnList.Count - 1]
        //            : new List<FileInfo> { new FileInfo(FullName) };

        //        var levelSplits = new List<FileInfo>();
        //        foreach (var file in filesToSplit)
        //        {
        //            levelSplits.AddRange(Split(splitOption.Method, splitOption.PrimaryValue,
        //                splitOption.SecondaryValue, splitOption.Randomize, splitOption.MaxSplits,
        //                exportDirectoryPath, file.FullName));
        //        }
        //        rtnList.Add(levelSplits);
        //    }
        //    rtnList.Reverse();
        //    return rtnList;
        //}


        //private List<FileInfo> Split(SplitMethod splitBy, object valueA, object valueB,
        //    bool randomize,
        //    object maxSplits, string newDirectory, string filePath = null)
        //{
        //    var file = string.IsNullOrEmpty(filePath)
        //        ? this
        //        : !IsFixedWidth ? new DataFileInfo(filePath, Layout.HasColumnHeaders) : new DataFileInfo(filePath, Layout);
        //    if (!string.IsNullOrEmpty(newDirectory))
        //    {
        //        var newdir = new DirectoryInfo(newDirectory);
        //        if (!newdir.Exists)
        //        {
        //            newdir.Create();
        //        }
        //        if (!newDirectory.EndsWith(@"\"))
        //        {
        //            newDirectory += @"\";
        //        }
        //    }
        //    var query = CreateQuery();
        //    var fileList = new List<string>();
        //    const string partSuffix = "_Part";
        //    const string incrementPlaceHolder = "[increment]";
        //    const string countColumnName = "____COUNT";
        //    var templateFileName = (string.IsNullOrEmpty(newDirectory) ? file.DirectoryName + @"\" : newDirectory) +
        //                           file.NameWithoutExtension + partSuffix + incrementPlaceHolder + file.Extension;
        //    SqlDataReader dr = null;
        //    StreamReader reader = null;
        //    StreamWriter writer = null;
        //    try
        //    {
        //        string targetFileName;
        //        string line;
        //        var increment = 1;
        //        string sqlColString;
        //        long recordCount;
        //        var columnLine = file.GetFileColumnString();
        //        switch (splitBy)
        //        {
        //            case SplitMethod.ByField:
        //                var columnsWithAliases = valueA.ToString();
        //                var columnsWithoutAliases = valueB.ToString();
        //                sqlColString = columnsWithAliases + ", COUNT(*) AS '" + countColumnName + "'";
        //                targetFileName = GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
        //                writer = new StreamWriter(targetFileName);
        //                fileList.Add(targetFileName);
        //                reader = File.OpenText(file.FullName);
        //                if (file.Layout.HasColumnHeaders)
        //                {
        //                    writer.WriteLine(columnLine);
        //                    reader.ReadLine(); //Skip columns
        //                }
        //                reader.Close();
        //                query.Select(sqlColString).GroupBy(columnsWithoutAliases).OrderBy("COUNT(*)");
        //                var counts = file.ToDataTable(query);
        //                for (var x = 0; x < counts.Layout.Columns.Count; x++)
        //                {
        //                    var column = counts.Layout.Columns[x];
        //                    if (column.ColumnName != countColumnName) continue;
        //                    counts.Layout.Columns.RemoveAt(x);
        //                    break;
        //                }
        //                for (var split = 0; split < counts.Rows.Count; split++)
        //                {
        //                    var row = counts.Rows[split];
        //                    var where = "";
        //                    for (var x = 0; x < counts.Layout.Columns.Count; x++)
        //                    {
        //                        var column = counts.Layout.Columns[x];
        //                        var value = row[x].ToString();
        //                        where += column.ColumnName + " = '" + value + "'";
        //                        if (x != counts.Layout.Columns.Count - 1)
        //                        {
        //                            where += " AND ";
        //                        }
        //                    }

        //                    query.Where(where);
        //                    if (randomize)
        //                    {
        //                        query.Shuffle();
        //                    }
        //                    dr = file.ToSqlDataReader(query);
        //                    while (dr.Read())
        //                    {
        //                        var rowString = "";
        //                        for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                        {
        //                            rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                            if (y != file.Layout.Columns.Count - 1)
        //                            {
        //                                rowString += file.Layout.FieldDelimiter;
        //                            }
        //                        }
        //                        writer.WriteLine(rowString);
        //                    }
        //                    dr.Close();
        //                    if ((maxSplits != null && increment >= Convert.ToInt32(maxSplits)) || split == counts.Rows.Count - 1) continue;
        //                    increment++;
        //                    targetFileName =
        //                        GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                            increment.ToString()));
        //                    writer.Flush();
        //                    writer.Close();
        //                    writer = new StreamWriter(targetFileName);
        //                    fileList.Add(targetFileName);
        //                    if (file.Layout.HasColumnHeaders)
        //                    {
        //                        writer.WriteLine(columnLine);
        //                    }
        //                }
        //                break;
        //            case SplitMethod.ByFileQuery:
        //                var whereClause = valueA.ToString();
        //                targetFileName =
        //                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
        //                writer = new StreamWriter(targetFileName);
        //                fileList.Add(targetFileName);
        //                reader = File.OpenText(file.FullName);
        //                if (file.Layout.HasColumnHeaders)
        //                {
        //                    writer.WriteLine(columnLine);
        //                    reader.ReadLine(); //Skip columns
        //                }
        //                reader.Close();
        //                query.Where(whereClause);
        //                if (randomize)
        //                {
        //                    query.Shuffle();
        //                }
        //                dr = file.ToSqlDataReader(query, true, Guid.NewGuid().ToString("N"));
        //                while (dr.Read())
        //                {
        //                    var rowString = "";
        //                    for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                    {
        //                        rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                        if (y != file.Layout.Columns.Count - 1)
        //                        {
        //                            rowString += file.Layout.FieldDelimiter;
        //                        }
        //                    }
        //                    writer.WriteLine(rowString);
        //                }
        //                dr.Close();
        //                writer.Flush();
        //                writer.Close();

        //                if (randomize)
        //                {
        //                    query.Shuffle();
        //                }
        //                dr = file.ToSqlDataReader(query, true, Guid.NewGuid().ToString("N"));

        //                if (dr.Read())
        //                {
        //                    increment++;
        //                    targetFileName =
        //                        GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                            increment.ToString()));
        //                    writer = new StreamWriter(targetFileName);
        //                    fileList.Add(targetFileName);
        //                    if (file.Layout.HasColumnHeaders)
        //                    {
        //                        writer.WriteLine(columnLine);
        //                    }
        //                    do
        //                    {
        //                        var rowString = "";
        //                        for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                        {
        //                            rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                            if (y != file.Layout.Columns.Count - 1)
        //                            {
        //                                rowString += file.Layout.FieldDelimiter;
        //                            }
        //                        }
        //                        writer.WriteLine(rowString);
        //                    } while (dr.Read());
        //                }
        //                break;
        //            case SplitMethod.ByFileSize:
        //                var maxFileSize = Convert.ToInt64(valueA);
        //                targetFileName =
        //                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
        //                writer = new StreamWriter(targetFileName);
        //                fileList.Add(targetFileName);
        //                reader = File.OpenText(file.FullName);
        //                long fileSize = 0;
        //                long columnLineSize = 0;
        //                if (file.Layout.HasColumnHeaders)
        //                {
        //                    columnLineSize = Encoding.UTF8.GetByteCount(columnLine);
        //                    fileSize += columnLineSize;
        //                    writer.WriteLine(columnLine);
        //                    reader.ReadLine(); //Skip columns
        //                }
        //                if (randomize)
        //                {
        //                    reader.Close();
        //                    query.Shuffle();

        //                    dr = file.ToSqlDataReader(query);
        //                    while (dr.Read())
        //                    {
        //                        var rowString = "";
        //                        for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                        {
        //                            rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                            if (y != file.Layout.Columns.Count - 1)
        //                            {
        //                                rowString += file.Layout.FieldDelimiter;
        //                            }
        //                        }
        //                        var rowSize = Encoding.UTF8.GetByteCount(rowString);
        //                        fileSize += rowSize;
        //                        if (fileSize <= maxFileSize ||
        //                            (maxSplits != null && increment >= Convert.ToInt32(maxSplits)))
        //                        {
        //                            writer.WriteLine(rowString);
        //                        }
        //                        else
        //                        {
        //                            fileSize = 0;
        //                            increment++;
        //                            targetFileName =
        //                                GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                    increment.ToString()));
        //                            writer.Flush();
        //                            writer.Close();
        //                            writer = new StreamWriter(targetFileName);
        //                            fileList.Add(targetFileName);
        //                            if (file.Layout.HasColumnHeaders)
        //                            {
        //                                writer.WriteLine(columnLine);
        //                                fileSize += columnLineSize;
        //                            }
        //                            writer.WriteLine(rowString);
        //                            fileSize += rowSize;
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    line = reader.ReadLine();
        //                    while (line != null)
        //                    {
        //                        var rowSize = Encoding.UTF8.GetByteCount(line);
        //                        fileSize += rowSize;
        //                        if (fileSize <= maxFileSize ||
        //                            (maxSplits != null && increment >= Convert.ToInt32(maxSplits)))
        //                        {
        //                            writer.WriteLine(line);
        //                        }
        //                        else
        //                        {
        //                            fileSize = 0;
        //                            increment++;
        //                            targetFileName =
        //                                GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                    increment.ToString()));
        //                            writer.Flush();
        //                            writer.Close();
        //                            writer = new StreamWriter(targetFileName);
        //                            fileList.Add(targetFileName);
        //                            if (file.Layout.HasColumnHeaders)
        //                            {
        //                                writer.WriteLine(columnLine);
        //                                fileSize += columnLineSize;
        //                            }
        //                            writer.WriteLine(line);
        //                            fileSize += rowSize;
        //                        }
        //                        line = reader.ReadLine();
        //                    }
        //                }
        //                break;
        //            case SplitMethod.ByMaxRecords:
        //                var maxRecords = Convert.ToInt32(valueA);
        //                recordCount = 0;
        //                targetFileName =
        //                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
        //                writer = new StreamWriter(targetFileName);
        //                fileList.Add(targetFileName);
        //                if (file.Layout.HasColumnHeaders)
        //                {
        //                    writer.WriteLine(columnLine);
        //                }
        //                reader = File.OpenText(file.FullName);
        //                if (randomize)
        //                {
        //                    reader.Close();
        //                    query.Shuffle();
        //                    dr = file.ToSqlDataReader(query);
        //                    while (dr.Read())
        //                    {
        //                        var rowString = "";
        //                        for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                        {
        //                            rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                            if (y != file.Layout.Columns.Count - 1)
        //                            {
        //                                rowString += file.Layout.FieldDelimiter;
        //                            }
        //                        }
        //                        writer.WriteLine(rowString);
        //                        recordCount++;
        //                        if (recordCount < maxRecords ||
        //                            (maxSplits != null && increment >= Convert.ToInt32(maxSplits))) continue;
        //                        recordCount = 0;
        //                        increment++;
        //                        targetFileName =
        //                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                increment.ToString()));
        //                        writer.Flush();
        //                        writer.Close();
        //                        writer = new StreamWriter(targetFileName);
        //                        fileList.Add(targetFileName);
        //                        if (file.Layout.HasColumnHeaders)
        //                        {
        //                            writer.WriteLine(columnLine);
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    if (file.Layout.HasColumnHeaders)
        //                    {
        //                        reader.ReadLine(); //Skip Layout.Columns
        //                    }
        //                    line = reader.ReadLine();
        //                    while (line != null)
        //                    {
        //                        writer.WriteLine(line);
        //                        line = reader.ReadLine();
        //                        recordCount++;
        //                        if (recordCount < maxRecords ||
        //                            (maxSplits != null && increment >= Convert.ToInt32(maxSplits))) continue;
        //                        recordCount = 0;
        //                        increment++;
        //                        targetFileName =
        //                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                increment.ToString()));
        //                        writer.Flush();
        //                        writer.Close();
        //                        writer = new StreamWriter(targetFileName);
        //                        fileList.Add(targetFileName);
        //                        if (file.Layout.HasColumnHeaders)
        //                        {
        //                            writer.WriteLine(columnLine);
        //                        }
        //                    }
        //                }
        //                break;
        //            case SplitMethod.ByPercentage:
        //                var percentage = Convert.ToDouble(valueA);
        //                var partitionField = valueB.ToString();
        //                var fileSplits = Convert.ToInt32(Math.Floor(100 / percentage)).ToString();
        //                var sqlColumnString = "TODO";
        //                var randomizer = "NEWID()";
        //                sqlColString = sqlColumnString + ",NTILE(" + fileSplits + ") OVER(PARTITION BY " +
        //                               partitionField + " ORDER BY " +
        //                               (randomize ? randomizer : "iFileSessionRecordId") + " DESC) AS FileGroup";
        //                query.Select(sqlColString).GroupBy("FileGroup");
        //                dr = file.ToSqlDataReader(query);
        //                var fileGroupColumnIndex = file.Layout.Columns.Count;
        //                var currGroup = "";
        //                while (dr.Read())
        //                {
        //                    var fileGroup = dr[fileGroupColumnIndex].ToString();
        //                    if (currGroup != fileGroup && (maxSplits == null || increment <= Convert.ToInt32(maxSplits)))
        //                    {
        //                        targetFileName =
        //                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                increment.ToString()));
        //                        if (writer != null)
        //                        {
        //                            writer.Flush();
        //                            writer.Close();
        //                        }
        //                        writer = new StreamWriter(targetFileName);
        //                        fileList.Add(targetFileName);
        //                        if (file.Layout.HasColumnHeaders)
        //                        {
        //                            writer.WriteLine(columnLine);
        //                        }
        //                        increment++;
        //                        currGroup = fileGroup;
        //                    }
        //                    var rowString = "";
        //                    for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                    {
        //                        rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                        if (y != file.Layout.Columns.Count - 1)
        //                        {
        //                            rowString += file.Layout.FieldDelimiter;
        //                        }
        //                    }
        //                    if (writer != null) writer.WriteLine(rowString);
        //                }
        //                break;
        //            case SplitMethod.ByParts:
        //                var totalParts = Convert.ToInt32(valueA);
        //                maxSplits = totalParts;
        //                int calculatedMaxRecords;
        //                var totalRecordsWritten = 0;
        //                recordCount = 0;
        //                targetFileName =
        //                    GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder, increment.ToString()));
        //                writer = new StreamWriter(targetFileName);
        //                fileList.Add(targetFileName);
        //                if (file.Layout.HasColumnHeaders)
        //                {
        //                    writer.WriteLine(columnLine);
        //                }
        //                if (randomize)
        //                {
        //                    calculatedMaxRecords =
        //                        Convert.ToInt32(Math.Round((double)file.TotalRecords / totalParts));
        //                    query.Shuffle();
        //                    dr = file.ToSqlDataReader(query);
        //                    while (dr.Read())
        //                    {
        //                        var rowString = "";
        //                        for (var y = 0; y < file.Layout.Columns.Count; y++)
        //                        {
        //                            rowString += file.ModifyValueBasedOnSettings(dr[y].ToString());
        //                            if (y != file.Layout.Columns.Count - 1)
        //                            {
        //                                rowString += file.Layout.FieldDelimiter;
        //                            }
        //                        }
        //                        writer.WriteLine(rowString);
        //                        totalRecordsWritten++;
        //                        recordCount++;
        //                        if (recordCount < calculatedMaxRecords ||
        //                            (increment >= Convert.ToInt32(maxSplits)) ||
        //                            totalRecordsWritten == file.TotalRecords) continue;
        //                        recordCount = 0;
        //                        increment++;
        //                        targetFileName =
        //                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                increment.ToString()));
        //                        writer.Flush();
        //                        writer.Close();
        //                        writer = new StreamWriter(targetFileName);
        //                        fileList.Add(targetFileName);
        //                        if (file.Layout.HasColumnHeaders)
        //                        {
        //                            writer.WriteLine(columnLine);
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    reader = File.OpenText(file.FullName);
        //                    calculatedMaxRecords = Convert.ToInt32(Math.Round((double)file.TotalRecords / totalParts));
        //                    if (file.Layout.HasColumnHeaders)
        //                    {
        //                        reader.ReadLine(); //Skip Layout.Columns
        //                    }
        //                    line = reader.ReadLine();
        //                    while (line != null)
        //                    {
        //                        writer.WriteLine(line);
        //                        line = reader.ReadLine();
        //                        recordCount++;
        //                        totalRecordsWritten++;
        //                        if (recordCount < calculatedMaxRecords ||
        //                            (increment >= Convert.ToInt32(maxSplits)) ||
        //                            totalRecordsWritten == file.TotalRecords) continue;
        //                        recordCount = 0;
        //                        increment++;
        //                        targetFileName =
        //                            GetNextAvailableName(templateFileName.Replace(incrementPlaceHolder,
        //                                increment.ToString()));
        //                        writer.Flush();
        //                        writer.Close();
        //                        writer = new StreamWriter(targetFileName);
        //                        fileList.Add(targetFileName);
        //                        if (file.Layout.HasColumnHeaders)
        //                        {
        //                            writer.WriteLine(columnLine);
        //                        }
        //                    }
        //                }
        //                break;
        //        }
        //    }
        //    finally
        //    {
        //        if (reader != null)
        //        {
        //            reader.Close();
        //        }
        //        if (writer != null)
        //        {
        //            writer.Flush();
        //            writer.Close();
        //        }
        //        if (dr != null)
        //        {
        //            dr.Close();
        //            dr.Dispose();
        //        }
        //    }
        //    var rtnList = new List<FileInfo>();
        //    foreach (var path in fileList)
        //    {
        //        rtnList.Add(new FileInfo(path));
        //    }
        //    return rtnList;
        //}
       
    }
}