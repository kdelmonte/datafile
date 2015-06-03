using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models.Query
{
    public class Expression
    {
        public Column Column { get; set; }
        public string Literal{ get; set; }

        public Expression(Column column)
        {
            Column = column;
        }

        public Expression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
