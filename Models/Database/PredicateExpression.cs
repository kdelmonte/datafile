using System;
using DataFile.Interfaces;

namespace DataFile.Models.Database
{
    public class PredicateExpression : IDataFileQueryPredicate
    {
        public Expression ColumnExpression { get; set; }
        public object Value { get; set; }
        public ComparisonOperator ComparisonOperator { get; set; }
        public ConjunctionOperator ConjunctionOperator { get; set; }
        public string Literal { get; set; }
        

        public PredicateExpression()
        {
            ConjunctionOperator = ConjunctionOperator.And;
            ComparisonOperator = ComparisonOperator.Equals;
        }

        public PredicateExpression(DataFileColumn column, ComparisonOperator comparisonOperator, object value): this()
        {
            ColumnExpression = new Expression(column);
            Value = value;
            ComparisonOperator = comparisonOperator;
        }

        public PredicateExpression(string columnName, ComparisonOperator comparisonOperator, object value)
            : this(new DataFileColumn(columnName), comparisonOperator, value)
        {
        }

        public PredicateExpression(string format, params object[] args)
            : this()
        {
            Literal = string.Format(format, args);
        }
    }
}
