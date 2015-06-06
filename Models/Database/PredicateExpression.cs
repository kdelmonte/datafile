using System;

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

        public PredicateExpression(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            ColumnExpression = new Expression(column);
            Value = value;
            ComparisonOperator = comparisonOperator;
        }

        public PredicateExpression(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            ColumnExpression = new Expression(new DataFileColumn(columnName));
            Value = value;
            ComparisonOperator = comparisonOperator;
        }

        public PredicateExpression(string format, params object[] args)
            : this()
        {
            Literal = string.Format(format, args);
        }
    }
}
