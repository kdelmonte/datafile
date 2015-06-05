using System.Collections.Generic;
using System.Linq;
using DataFile.Models.Database.Interfaces;
using Newtonsoft.Json;

namespace DataFile.Models.Database
{
    public class DatabaseCommand
    {
        public long? RowLimit { get; private set; }
        public List<Expression> SelectExpressions { get; private set; }
        public List<UpdateExpression> UpdateExpressions { get; private set; }
        public InsertIntoExpression InsertIntoExpression { get; private set; }
        public List<OrderByExpression> OrderByExpressions { get; private set; }
        public List<Expression> GroupByExpressions { get; private set; }
        public List<FilterExpression> QueryFilters { get; private set; }
        public List<ColumnModificationExpression> AlterExpressions { get; private set; }
        public DataFileInfo SourceFile { get; private set; }
        public bool Shuffling { get; private set; }
        public IDatabaseInterface Interface { get; set; }
        public FilterClauseType LastFilterClauseUsed { get; private set; }
        public DatabaseCommandMode Mode { get; private set; }

        public string GetSelectClause()
        {
            return Interface.BuildSelectClause(this);
        }

        public string GetInsertIntoClause()
        {
            return Interface.BuildInsertIntoClause(this);
        }

        public string GetUpdateClause()
        {
            return Interface.BuildUpdateClause(this);
        }

        public string GetOrderByClause()
        {
            return Interface.BuildOrderByClause(this);
        }

        public string GetGroupByClause()
        {
            return Interface.BuildGroupByClause(this);
        }

        public string GetWhereClause()
        {
            return Interface.BuildFilterClause(this, FilterClauseType.Where);
        }

        public string GetHavingClause()
        {
            return Interface.BuildFilterClause(this, FilterClauseType.Having);
        }

        public DatabaseCommand()
        {
            SelectExpressions = new List<Expression>();
            UpdateExpressions = new List<UpdateExpression>();
            OrderByExpressions = new List<OrderByExpression>();
            GroupByExpressions = new List<Expression>();
            QueryFilters = new List<FilterExpression>();
            AlterExpressions = new List<ColumnModificationExpression>();
        }

        public DatabaseCommand(IDatabaseInterface dbInterface) : this()
        {
            Interface = dbInterface;
        }

        public string ToQuery()
        {
            return Interface.BuildQuery(this);
        }

        public DatabaseCommand From(DataFileInfo sourceFile)
        {
            if (sourceFile == null)
            {
                SourceFile = null;
                return this;
            }
            SourceFile = sourceFile;
            return this;
        }

        public DatabaseCommand Select(IEnumerable<Expression> selectColumns)
        {
            if (SelectExpressions == null)
            {
                SelectExpressions = null;
                return this;
            }
            Mode = DatabaseCommandMode.Select;
            SelectExpressions = selectColumns.ToList();
            return this;
        }

        public DatabaseCommand Select(IEnumerable<DataFileColumn> selectColumns)
        {
            var expressions = selectColumns.Select(column => new Expression(column));
            return Select(expressions);
        }

        public DatabaseCommand Select(DataFileColumn selectColumn)
        {
            return Select(new List<DataFileColumn> { selectColumn });
        }

        public DatabaseCommand Select(string format, params object[] args)
        {
            var expression = new Expression(format, args);
            return Select(new List<Expression> {expression});
        }

        public DatabaseCommand Delete()
        {
            Mode = DatabaseCommandMode.Delete;
            return this;
        }

        public DatabaseCommand Update(IEnumerable<UpdateExpression> updateExpressions)
        {
            if (updateExpressions == null)
            {
                UpdateExpressions = null;
                return this;
            }
            Mode = DatabaseCommandMode.Update;
            UpdateExpressions = updateExpressions.ToList();
            return this;
        }

        public DatabaseCommand Update(UpdateExpression updateExpression)
        {
            UpdateExpressions.Add(updateExpression);
            return Update(UpdateExpressions);
        }

        public DatabaseCommand Update(string format, params object[] args)
        {
            var expression = new UpdateExpression(format, args);
            return Update(new List<UpdateExpression> {expression});
        }

        public DatabaseCommand Update(DataFileColumn column, object value)
        {
            var expression = new UpdateExpression
            {
                ColumnExpression = new Expression(column),
                Value = value
            };
            return Update(expression);
        }

        public DatabaseCommand Set(DataFileColumn column, object value)
        {
            return Update(column, value);
        }

        public DatabaseCommand Alter(IEnumerable<ColumnModificationExpression> modificationExpressions)
        {
            if (modificationExpressions == null)
            {
                AlterExpressions = null;
                return this;
            }
            Mode = DatabaseCommandMode.Alter;
            AlterExpressions = modificationExpressions.ToList();
            return this;
        }

        public DatabaseCommand Alter(ColumnModificationExpression modificationExpression)
        {
            AlterExpressions.Add(modificationExpression);
            return Alter(AlterExpressions);
        }

        public DatabaseCommand Alter(ColumnModificationType modifcationType,string format, params object[] args)
        {
            var expression = new ColumnModificationExpression(modifcationType, format, args);
            return Alter(new List<ColumnModificationExpression> { expression });
        }

        public DatabaseCommand Alter(ColumnModificationType modifcationType, DataFileColumn column)
        {
            var expression = new ColumnModificationExpression
            {
                Column = column,
                ModificationType = modifcationType
            };
            return Alter(expression);
        }

        public DatabaseCommand Alter(ColumnModificationType modifcationType, List<DataFileColumn> columns)
        {
            foreach (var column in columns)
            {
                Alter(modifcationType, column);
            }
            return this;
        }

        public DatabaseCommand InsertInto(InsertIntoExpression insertIntoExpression)
        {
            if (insertIntoExpression == null)
            {
                InsertIntoExpression = null;
                return this;
            }
            Mode = DatabaseCommandMode.Insert;
            InsertIntoExpression = insertIntoExpression;
            return this;
        }

        public DatabaseCommand InsertInto(string format, params object[] args)
        {
            var expression = new InsertIntoExpression(format, args);
            return InsertInto(expression);
        }

        public DatabaseCommand Limit(long? limit)
        {
            RowLimit = limit;
            return this;
        }

        public DatabaseCommand OrderBy(IEnumerable<OrderByExpression> orderByExpression)
        {
            if (orderByExpression == null)
            {
                OrderByExpressions = null;
                return this;
            }
            OrderByExpressions = orderByExpression.ToList();
            return this;
        }

        public DatabaseCommand OrderBy(string format, params object[] args)
        {
            var expression = new OrderByExpression(format, args);
            return OrderBy(new List<OrderByExpression> {expression});
        }

        public DatabaseCommand GroupBy(IEnumerable<Expression> groupByExpression)
        {
            if (groupByExpression == null)
            {
                GroupByExpressions = null;
                return this;
            }
            GroupByExpressions = groupByExpression.ToList();
            return this;
        }

        public DatabaseCommand GroupBy(IEnumerable<DataFileColumn> selectColumns)
        {
            var expressions = selectColumns.Select(column => new Expression(column));
            return GroupBy(expressions);
        }

        public DatabaseCommand GroupBy(string format, params object[] args)
        {
            var expression = new Expression(format, args);
            return GroupBy(new List<Expression> {expression});
        }

        public DatabaseCommand Where(FilterExpression filterExpression)
        {
            LastFilterClauseUsed = FilterClauseType.Where;
            filterExpression.TargetClause = FilterClauseType.Where;
            filterExpression.ConjunctionOperator = ConjunctionOperator.And;
            QueryFilters.Add(filterExpression);
            return this;
        }

        public DatabaseCommand Where(string format, params object[] args)
        {
            var expression = new FilterExpression(format, args);
            return Where(expression);
        }

        public DatabaseCommand Where(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = CreateFilterExpression(column, value, comparisonOperator);
            return Where(expression);
        }

        public DatabaseCommand Having(FilterExpression filterExpression)
        {
            LastFilterClauseUsed = FilterClauseType.Having;
            filterExpression.TargetClause = FilterClauseType.Having;
            filterExpression.ConjunctionOperator = ConjunctionOperator.And;
            QueryFilters.Add(filterExpression);
            return this;
        }

        public DatabaseCommand Having(string format, params object[] args)
        {
            var expression = new FilterExpression(format, args);
            return Having(expression);
        }

        public DatabaseCommand Having(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = CreateFilterExpression(column, value, comparisonOperator);
            return Having(expression);
        }

        public DatabaseCommand And(FilterExpression filterExpression)
        {
            filterExpression.TargetClause = LastFilterClauseUsed;
            filterExpression.ConjunctionOperator = ConjunctionOperator.And;
            QueryFilters.Add(filterExpression);
            return this;
        }

        public DatabaseCommand And(string format, params object[] args)
        {
            var expression = new FilterExpression(format, args);
            return And(expression);
        }

        public DatabaseCommand And(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = CreateFilterExpression(column, value, comparisonOperator);
            return And(expression);
        }

        public DatabaseCommand Or(FilterExpression filterExpression)
        {
            filterExpression.TargetClause = LastFilterClauseUsed;
            filterExpression.ConjunctionOperator = ConjunctionOperator.Or;
            QueryFilters.Add(filterExpression);
            return this;
        }

        public DatabaseCommand Or(string format, params object[] args)
        {
            var expression = new FilterExpression(format, args);
            return Or(expression);
        }

        public DatabaseCommand Or(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = CreateFilterExpression(column, value, comparisonOperator);
            return Or(expression);
        }

        public DatabaseCommand Shuffle()
        {
            Shuffling = true;
            return this;
        }

        public DatabaseCommand Deshuffle()
        {
            Shuffling = false;
            return this;
        }

        public DatabaseCommand ToggleShuffle()
        {
            Shuffling = !Shuffling;
            return this;
        }

        public List<FilterExpression> GetQueryFiltersByTargetClause(FilterClauseType clauseType)
        {
            return QueryFilters.Where(filter => filter.TargetClause.Equals(clauseType)).ToList();
        }

        public DatabaseCommand ClearWhereClause()
        {
            QueryFilters.RemoveAll(filter => filter.TargetClause.Equals(FilterClauseType.Where));
            return this;
        }

        public DatabaseCommand ClearHavingClause()
        {
            QueryFilters.RemoveAll(filter => filter.TargetClause.Equals(FilterClauseType.Having));
            return this;
        }

        public DatabaseCommand Clone()
        {
            var serializable = new
            {
                RowLimit,
                SelectExpressions,
                UpdateExpressions,
                InsertIntoExpression,
                OrderByExpressions,
                GroupByExpressions,
                QueryFilters,
                AlterExpressions,
                SourceFile,
                Shuffling,
                LastFilterClauseUsed,
                Mode
            };
            var clone = CloneObject<DatabaseCommand>(serializable);
            clone.Interface = Interface;
            clone.RowLimit = RowLimit;
            clone.SourceFile = SourceFile;
            clone.InsertIntoExpression = CloneObject<InsertIntoExpression>(InsertIntoExpression);
            clone.Shuffling = Shuffling;
            clone.LastFilterClauseUsed = LastFilterClauseUsed;
            clone.Mode = Mode;

            return clone;
        }

    private static T CloneObject<T>(object obj)
        {
            var serialized = JsonConvert.SerializeObject(obj);
            var clone = JsonConvert.DeserializeObject<T>(serialized);
            return clone;
        }

        private static FilterExpression CreateFilterExpression(DataFileColumn column, object value,
            ComparisonOperator comparisonOperator)
        {
            return new FilterExpression
            {
                ColumnExpression = new Expression(column),
                Value = value,
                ComparisonOperator = comparisonOperator
            };
        }
    }
}
