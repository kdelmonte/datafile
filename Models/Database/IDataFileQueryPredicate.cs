using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataFile.Models.Database
{
    public interface IDataFileQueryPredicate
    {
        ConjunctionOperator ConjunctionOperator { get; set; }
    }
}
