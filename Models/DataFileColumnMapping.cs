namespace DataFile.Models
{
    public class DataFileColumnMapping
    {
        public int Source;
        public int Target;

        public DataFileColumnMapping(int sourceIndex, int targetIndex)
        {
            Source = sourceIndex;
            Target = targetIndex;
        }

        public DataFileColumnMapping()
        {
        }
    }
}