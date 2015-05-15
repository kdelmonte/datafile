namespace Fable
{
    public class ColumnMapping
    {
        public int SourceFieldIndex;
        public int TargetFieldIndex;

        public ColumnMapping(int sourceIndex, int targetIndex)
        {
            SourceFieldIndex = sourceIndex;
            TargetFieldIndex = targetIndex;
        }

        public ColumnMapping()
        {
        }
    }
}