namespace DataFile.Models.Database
{
    public class Expression
    {
        public DataFileColumn Column { get; set; }
        public string Literal{ get; set; }

        public Expression()
        {
           
        }

        public Expression(DataFileColumn column)
        {
            Column = column;
        }

        public Expression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
