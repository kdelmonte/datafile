using System.Collections.Generic;

namespace DataFile.Models
{
    public static class DataFileExtension
    {
        private const string DefaultExtension = ".txt";
        private static readonly Dictionary<DataFileFormat, string> FileExtensions = new Dictionary<DataFileFormat, string>
        {
            {DataFileFormat.CommaDelimited, ".csv"},
            {DataFileFormat.TabDelimited, DefaultExtension},
            {DataFileFormat.PipeDelimited, DefaultExtension},
            {DataFileFormat.CharacterDelimited, DefaultExtension},
            {DataFileFormat.XLS, ".xls"},
            {DataFileFormat.XLSX, ".xlsx"},
            {DataFileFormat.DatabaseImport, DataFileInfo.DatabaseImportFileExtension}
        };

        public static string ByFormat(DataFileFormat format)
        {
            string extension;
            return FileExtensions.TryGetValue(format, out extension) ? extension : DefaultExtension;
        }
    }
}
