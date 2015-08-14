namespace DataFile.Models.Database
{
    public class ColumnModificationExpression
    {
        public ColumnModificationType ModificationType { get; set; }
        public DataFileColumn Column;
        public string Literal { get; set; }

        public ColumnModificationExpression()
        {
            
        }

        public ColumnModificationExpression(ColumnModificationType modificationType)
        {
            ModificationType = modificationType;
        }

        public ColumnModificationExpression(ColumnModificationType modificationType, DataFileColumn column)
            : this(modificationType)
        {
            Column = column;
        }

        public ColumnModificationExpression(ColumnModificationType modificationType, string format, params object[] args)
            : this(modificationType)
        {
            Literal = string.Format(format, args);
        }
    }
}
