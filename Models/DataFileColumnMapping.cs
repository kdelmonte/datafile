namespace DataFile.Models
{
    public class DataFileColumnMapping
    {
        public int SourceFieldIndex;
        public int TargetFieldIndex;

        public DataFileColumnMapping(int sourceIndex, int targetIndex)
        {
            SourceFieldIndex = sourceIndex;
            TargetFieldIndex = targetIndex;
        }

        public DataFileColumnMapping()
        {
        }
    }
}