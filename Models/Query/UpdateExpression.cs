using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models.Query
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
