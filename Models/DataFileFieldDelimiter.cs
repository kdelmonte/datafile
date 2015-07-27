using System.Collections.Generic;

namespace DataFile.Models
{
    public static class DataFileFieldDelimiter
    {
        private static readonly Dictionary<DataFileFormat, string> FieldDelimiters = new Dictionary<DataFileFormat, string>
        {
            {DataFileFormat.CommaDelimited, ","},
            {DataFileFormat.TabDelimited, "\t"},
            {DataFileFormat.PipeDelimited, "|"},
            {DataFileFormat.DatabaseImport, DataFileInfo.ImportFieldDelimiter}
        };

        public static string ByFormat(DataFileFormat format)
        {
            string delimiter;
            return FieldDelimiters.TryGetValue(format, out delimiter) ? delimiter : null;
        }

        public static DataFileFormat? GetFormatByDelimiter(string delimiter)
        {
            foreach (var predefinedDelimiter in FieldDelimiters)
            {
                if (predefinedDelimiter.Value == delimiter)
                {
                    return predefinedDelimiter.Key;
                }
            }
            return null;
        }
    }
}
