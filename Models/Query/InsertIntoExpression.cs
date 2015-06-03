using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models.Query
{
    public class InsertIntoExpression
    {
        public List<Expression> ColumnExpressions { get; set; }
        public List<object> Values { get; set; }
        public string Literal { get; set; }

        public InsertIntoExpression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
