using System.Collections.Generic;

namespace DataFile.Models.Database
{
    public class InsertIntoExpression
    {
        public List<Expression> ColumnExpressions { get; set; }
        public List<object> Values { get; set; }
        public string Literal { get; set; }

        public InsertIntoExpression()
        {
            
        }

        public InsertIntoExpression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
