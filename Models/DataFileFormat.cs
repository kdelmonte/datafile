namespace DataFile.Models
{
    public enum DataFileFormat
    {
        Unknown = 0,
        DatabaseImport = 1,
        CharacterDelimited = 2,
        CommaDelimited = 3,
        XLSX = 4,
        XLS = 5,
        PipeDelimited = 6,
        SpaceDelimited = 7,
        TabDelimited = 8
    }
}