namespace DataFile.Models.Database
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
