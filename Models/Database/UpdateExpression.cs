namespace DataFile.Models.Database
{
    public class UpdateExpression
    {
        public Expression ColumnExpression { get; set; }
        public object Value { get; set; }
        public string Literal { get; set; }

        public UpdateExpression()
        {
            
        }

        public UpdateExpression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
