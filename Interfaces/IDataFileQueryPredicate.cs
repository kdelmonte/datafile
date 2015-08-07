using DataFile.Models.Database;

namespace DataFile.Interfaces
{
    public interface IDataFileQueryPredicate
    {
        ConjunctionOperator ConjunctionOperator { get; set; }
    }
}
