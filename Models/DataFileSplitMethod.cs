namespace DataFile.Models
{
    public enum DataFileSplitMethod
    {
        ByParts = 1,
        ByPercentage = 2,
        ByFileSize = 3,
        ByField = 4,
        ByMaxRecords = 5,
        ByFileQuery = 6
    }
}