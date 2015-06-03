using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models.Query
{
    public class OrderByExpression
    {
        public Expression ColumnExpression { get; set; }
        public string Literal { get; set; }
        public OrderByDirection Direction { get; set; }

        public OrderByExpression()
        {
            Direction = OrderByDirection.Asc;
        }

        public OrderByExpression(string format, params object[] args)
        {
            Literal = string.Format(format, args);
        }
    }
}
