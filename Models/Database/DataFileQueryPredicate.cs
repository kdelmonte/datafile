using System.Collections.Generic;

namespace DataFile.Models.Database
{
    public class DataFileQueryPredicate : IDataFileQueryPredicate
    {
        public List<IDataFileQueryPredicate> PredicateExpressions { get; set; }
        public PredicateClauseType TargetClause { get; set; }
        public ConjunctionOperator ConjunctionOperator { get; set; }

        public DataFileQueryPredicate()
        {
            PredicateExpressions = new List<IDataFileQueryPredicate>();
            ConjunctionOperator = ConjunctionOperator.And;
        }

        public DataFileQueryPredicate Where(DataFileQueryPredicate predicate)
        {
            return And(predicate);
        }

        public DataFileQueryPredicate Where(PredicateExpression predicateExpression)
        {
            return And(predicateExpression);
        }

        public DataFileQueryPredicate Where(string format, params object[] args)
        {
            return And(format,args);
        }

        public DataFileQueryPredicate Where(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            return And(column,comparisonOperator,value);
        }

        public DataFileQueryPredicate Where(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            return And(columnName,comparisonOperator,value);
        }

        public DataFileQueryPredicate And(DataFileQueryPredicate predicate)
        {
            predicate.ConjunctionOperator = ConjunctionOperator.And;
            PredicateExpressions.Add(predicate);
            return this;
        }

        public DataFileQueryPredicate And(PredicateExpression predicateExpression)
        {
            predicateExpression.ConjunctionOperator = ConjunctionOperator.And;
            PredicateExpressions.Add(predicateExpression);
            return this;
        }

        public DataFileQueryPredicate And(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return And(expression);
        }

        public DataFileQueryPredicate And(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return And(expression);
        }

        public DataFileQueryPredicate And(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return And(expression);
        }

        public DataFileQueryPredicate Or(DataFileQueryPredicate predicate)
        {
            predicate.ConjunctionOperator = ConjunctionOperator.Or;
            PredicateExpressions.Add(predicate);
            return this;
        }

        public DataFileQueryPredicate Or(PredicateExpression predicateExpression)
        {
            predicateExpression.ConjunctionOperator = ConjunctionOperator.Or;
            PredicateExpressions.Add(predicateExpression);
            return this;
        }

        public DataFileQueryPredicate Or(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return Or(expression);
        }

        public DataFileQueryPredicate Or(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return Or(expression);
        }

        public DataFileQueryPredicate Or(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return Or(expression);
        }
    }
}
