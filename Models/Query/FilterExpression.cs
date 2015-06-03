namespace DataFile.Models.Query
{
    public class FilterExpression
    {
        public Expression ColumnExpression { get; set; }
        public object Value { get; set; }
        public FilterClauseType TargetClause { get; set; }
        public ComparisonOperator ComparisonOperator { get; set; }
        public ConjunctionOperator ConjunctionOperator { get; set; }
        public string Literal { get; set; }
        

        public FilterExpression()
        {
            ConjunctionOperator = ConjunctionOperator.And;
            ComparisonOperator = ComparisonOperator.Equals;
        }

        public FilterExpression(string format, params object[] args): this()
        {
            Literal = string.Format(format, args);
        }
    }
}
