using System.Collections.Generic;
using System.Linq;
using DataFile.Interfaces;
using Newtonsoft.Json;

namespace DataFile.Models.Database
{
    public class DataFileQuery
    {
        public long? RowLimit { get; private set; }
        public List<Expression> SelectExpressions { get; private set; }
        public List<UpdateExpression> UpdateExpressions { get; private set; }
        public InsertIntoExpression InsertIntoExpression { get; private set; }
        public List<OrderByExpression> OrderByExpressions { get; private set; }
        public List<Expression> GroupByExpressions { get; private set; }
        public List<DataFileQueryPredicate> Predicates { get; private set; }
        public List<ColumnModificationExpression> AlterExpressions { get; private set; }
        public DataFileInfo SourceFile { get; set; }
        public bool Shuffling { get; private set; }
        public IDataFileDbAdapter Interface { get; set; }
        public PredicateClauseType LastPredicateClauseUsed { get; private set; }
        private DataFileQueryMode _mode;
        public DataFileQueryMode Mode
        {
            get { return _mode; }
            set
            {
                if (_mode == value) return;
                if (_mode == DataFileQueryMode.Undefined)
                {
                    _mode = value;
                    return;
                }
                Next = new DataFileQuery(_activeQuery.SourceFile, _activeQuery.Interface)
                {
                    Mode = value,
                    EntryPoint = _activeQuery.EntryPoint
                };
            }
        }

        private DataFileQuery _activeQuery;
        private DataFileQuery _entryPoint;
        private DataFileQuery EntryPoint
        {
            get
            {
                return _entryPoint ?? this;
            }
            set { _entryPoint = value; }
        }

        private DataFileQuery _next;
        private DataFileQuery Next
        {
            get
            {
                return _next;
            }
            set
            {
                _next = value;
                _activeQuery = value;
            }
        }

        public string GetSelectClause()
        {
            return _activeQuery.Interface.BuildSelectClause(_activeQuery);
        }

        public string GetInsertIntoClause()
        {
            return _activeQuery.Interface.BuildInsertIntoClause(_activeQuery);
        }

        public string GetUpdateClause()
        {
            return _activeQuery.Interface.BuildUpdateClause(_activeQuery);
        }

        public string GetOrderByClause()
        {
            return _activeQuery.Interface.BuildOrderByClause(_activeQuery);
        }

        public string GetGroupByClause()
        {
            return _activeQuery.Interface.BuildGroupByClause(_activeQuery);
        }

        public string GetWhereClause()
        {
            return _activeQuery.Interface.BuildPredicateClause(_activeQuery, PredicateClauseType.Where);
        }

        public string GetHavingClause()
        {
            return _activeQuery.Interface.BuildPredicateClause(_activeQuery, PredicateClauseType.Having);
        }

        public DataFileQuery()
        {
            _activeQuery = this;
            SelectExpressions = new List<Expression>();
            UpdateExpressions = new List<UpdateExpression>();
            OrderByExpressions = new List<OrderByExpression>();
            GroupByExpressions = new List<Expression>();
            Predicates = new List<DataFileQueryPredicate>();
            AlterExpressions = new List<ColumnModificationExpression>();
        }

        public DataFileQuery(DataFileInfo sourceFile,IDataFileDbAdapter dbInterface) : this()
        {
            SourceFile = sourceFile;
            Interface = dbInterface;
        }

        public List<DataFileQuery> GetQueries()
        {
            var queries = new List<DataFileQuery>();
            var query = _activeQuery.EntryPoint;
            while (query != null)
            {
                queries.Add(query);
                query = query.Next;
            }
            return queries;
        }

        public string ToQuery()
        {
            return Interface.BuildQuery(this);
        }

        public string ToQueryBatch()
        {
            var queries = GetQueries().Select(query => query.Interface.BuildQuery(query));
            return string.Join(Interface.QueryBatchSeparator, queries);
        }

        public DataFileQuery SetSourceFile(DataFileInfo sourceFile)
        {
            SourceFile = sourceFile;
            return this;
        }

        public DataFileQuery Select(IEnumerable<Expression> selectColumns)
        {
            _activeQuery.Mode = DataFileQueryMode.Select;
            _activeQuery.SelectExpressions = selectColumns.ToList();
            return _activeQuery;
        }

        public DataFileQuery Select(IEnumerable<DataFileColumn> selectColumns)
        {
            var expressions = selectColumns.Select(column => new Expression(column));
            return _activeQuery.Select(expressions);
        }

        public DataFileQuery Select(DataFileColumn selectColumn)
        {
            return _activeQuery.Select(new List<DataFileColumn> { selectColumn });
        }

        public DataFileQuery Select(string format, params object[] args)
        {
            var expression = new Expression(format, args);
            return _activeQuery.Select(new List<Expression> { expression });
        }

        public DataFileQuery Delete()
        {
            _activeQuery.Mode = DataFileQueryMode.Delete;
            return _activeQuery;
        }

        public DataFileQuery Update(IEnumerable<UpdateExpression> updateExpressions)
        {
            _activeQuery.Mode = DataFileQueryMode.Update;
            _activeQuery.UpdateExpressions = updateExpressions.ToList();
            return _activeQuery;
        }

        public DataFileQuery Update(UpdateExpression updateExpression)
        {
            _activeQuery.UpdateExpressions.Add(updateExpression);
            return _activeQuery.Update(UpdateExpressions);
        }

        public DataFileQuery Update(string format, params object[] args)
        {
            var expression = new UpdateExpression(format, args);
            return _activeQuery.Update(new List<UpdateExpression> { expression });
        }

        public DataFileQuery Update(DataFileColumn column, object value)
        {
            var expression = new UpdateExpression
            {
                ColumnExpression = new Expression(column),
                Value = value
            };
            return _activeQuery.Update(expression);
        }

        public DataFileQuery Update(IEnumerable<DataFileColumn> columns, IEnumerable<object> values)
        {
            var columnList = columns.ToList();
            var valuesList = values.ToList();
            for (var x = 0; x < columnList.Count; x++)
            {
                var column = columnList[x];
                var value = valuesList[x];
                var expression = new UpdateExpression
                {
                    ColumnExpression = new Expression(column),
                    Value = value
                };
                _activeQuery.Update(expression);
            }
            return _activeQuery;
        }

        public DataFileQuery Update(IEnumerable<string> columnNames, IEnumerable<object> values)
        {
            var columns = columnNames.Select(columnName => new DataFileColumn(columnName));
            return _activeQuery.Update(columns, values);
        }

        public DataFileQuery Update(Dictionary<DataFileColumn, object> updateDictionary)
        {
            var columns = updateDictionary.Select(item => item.Key);
            var values = updateDictionary.Select(item => item.Value);
            return _activeQuery.Update(columns, values);
        }

        public DataFileQuery Update(Dictionary<string, object> updateDictionary)
        {
            var columns = updateDictionary.Select(item => new DataFileColumn(item.Key));
            var values = updateDictionary.Select(item => item.Value);
            return _activeQuery.Update(columns, values);
        }

        public DataFileQuery Set(DataFileColumn column, object value)
        {
            return _activeQuery.Update(column, value);
        }

        public DataFileQuery Alter(IEnumerable<ColumnModificationExpression> modificationExpressions)
        {
            _activeQuery.Mode = DataFileQueryMode.Alter;
            _activeQuery.AlterExpressions = modificationExpressions.ToList();
            return _activeQuery;
        }

        public DataFileQuery Alter(ColumnModificationExpression modificationExpression)
        {
            AlterExpressions.Add(modificationExpression);
            return _activeQuery.Alter(AlterExpressions);
        }

        public DataFileQuery Alter(ColumnModificationType modifcationType,string format, params object[] args)
        {
            var expression = new ColumnModificationExpression(modifcationType, format, args);
            return _activeQuery.Alter(new List<ColumnModificationExpression> { expression });
        }

        public DataFileQuery Alter(ColumnModificationType modifcationType, DataFileColumn column)
        {
            var expression = new ColumnModificationExpression
            {
                Column = column,
                ModificationType = modifcationType
            };
            return _activeQuery.Alter(expression);
        }

        public DataFileQuery Alter(ColumnModificationType modifcationType, List<DataFileColumn> columns)
        {
            foreach (var column in columns)
            {
                _activeQuery.Alter(modifcationType, column);
            }
            return _activeQuery;
        }

        public DataFileQuery InsertInto(InsertIntoExpression insertIntoExpression)
        {
            _activeQuery.Mode = DataFileQueryMode.Insert;
            _activeQuery.InsertIntoExpression = insertIntoExpression;
            return _activeQuery;
        }

        public DataFileQuery InsertInto(string format, params object[] args)
        {
            var expression = new InsertIntoExpression(format, args);
            return _activeQuery.InsertInto(expression);
        }

        public DataFileQuery InsertInto(IEnumerable<DataFileColumn> columns, IEnumerable<object> values)
        {
            var expression = new InsertIntoExpression
            {
                ColumnExpressions = columns.Select(column => new Expression(column)).ToList(),
                Values = values.ToList()
            };
            return _activeQuery.InsertInto(expression);
        }

        public DataFileQuery InsertInto(IEnumerable<string> columnNames, IEnumerable<object> values)
        {
            var columns = columnNames.Select(columnName => new DataFileColumn(columnName));
            return _activeQuery.InsertInto(columns, values);
        }

        public DataFileQuery InsertInto(Dictionary<DataFileColumn, object> insertDictionary)
        {
            var columns = insertDictionary.Select(item => item.Key);
            var values = insertDictionary.Select(item => item.Value);
            return _activeQuery.InsertInto(columns, values);
        }

        public DataFileQuery InsertInto(Dictionary<string, object> insertDictionary)
        {
            var columns = insertDictionary.Select(item => new DataFileColumn(item.Key));
            var values = insertDictionary.Select(item => item.Value);
            return _activeQuery.InsertInto(columns, values);
        }

        public DataFileQuery Limit(long? limit)
        {
            _activeQuery.RowLimit = limit;
            return _activeQuery;
        }

        public DataFileQuery OrderBy(IEnumerable<OrderByExpression> orderByExpression)
        {
            _activeQuery.OrderByExpressions = orderByExpression.ToList();
            return _activeQuery;
        }

        public DataFileQuery OrderBy(string format, params object[] args)
        {
            var expression = new OrderByExpression(format, args);
            return _activeQuery.OrderBy(new List<OrderByExpression> { expression });
        }

        public DataFileQuery GroupBy(IEnumerable<Expression> groupByExpression)
        {
            _activeQuery.GroupByExpressions = groupByExpression.ToList();
            return _activeQuery;
        }

        public DataFileQuery GroupBy(IEnumerable<DataFileColumn> selectColumns)
        {
            var expressions = selectColumns.Select(column => new Expression(column));
            return _activeQuery.GroupBy(expressions);
        }

        public DataFileQuery GroupBy(string format, params object[] args)
        {
            var expression = new Expression(format, args);
            return _activeQuery.GroupBy(new List<Expression> { expression });
        }

        public DataFileQuery Where(DataFileQueryPredicate predicate)
        {
            _activeQuery.LastPredicateClauseUsed = PredicateClauseType.Where;
            predicate.TargetClause = PredicateClauseType.Where;
            predicate.ConjunctionOperator = ConjunctionOperator.And;
            _activeQuery.Predicates.Add(predicate);
            return _activeQuery;
        }

        public DataFileQuery Where(PredicateExpression predicateExpression)
        {
            var predicate = new DataFileQueryPredicate
            {
                PredicateExpressions = new List<IDataFileQueryPredicate> { predicateExpression }
            };
            return Where(predicate);
        }

        public DataFileQuery Where(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return _activeQuery.Where(expression);
        }

        public DataFileQuery Where(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return _activeQuery.Where(expression);
        }

        public DataFileQuery Where(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return _activeQuery.Where(expression);
        }

        public DataFileQuery Having(DataFileQueryPredicate predicate)
        {
            _activeQuery.LastPredicateClauseUsed = PredicateClauseType.Having;
            predicate.TargetClause = PredicateClauseType.Having;
            predicate.ConjunctionOperator = ConjunctionOperator.And;
            _activeQuery.Predicates.Add(predicate);
            return _activeQuery;
        }

        public DataFileQuery Having(PredicateExpression predicateExpression)
        {
            var predicate = new DataFileQueryPredicate
            {
                PredicateExpressions = new List<IDataFileQueryPredicate> { predicateExpression }
            };
            return Having(predicate);
        }

        public DataFileQuery Having(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return _activeQuery.Having(expression);
        }

        public DataFileQuery Having(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return _activeQuery.Having(expression);
        }

        public DataFileQuery Having(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return _activeQuery.Having(expression);
        }

        public DataFileQuery And(DataFileQueryPredicate predicate)
        {
            predicate.TargetClause = LastPredicateClauseUsed;
            predicate.ConjunctionOperator = ConjunctionOperator.And;
            _activeQuery.Predicates.Add(predicate);
            return _activeQuery;
        }

        public DataFileQuery And(PredicateExpression predicateExpression)
        {
            var predicate = new DataFileQueryPredicate
            {
                PredicateExpressions = new List<IDataFileQueryPredicate> { predicateExpression }
            };
            return And(predicate);
        }

        public DataFileQuery And(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return _activeQuery.And(expression);
        }

        public DataFileQuery And(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return _activeQuery.And(expression);
        }

        public DataFileQuery And(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return _activeQuery.And(expression);
        }

        public DataFileQuery Or(DataFileQueryPredicate predicate)
        {
            predicate.TargetClause = LastPredicateClauseUsed;
            predicate.ConjunctionOperator = ConjunctionOperator.Or;
            _activeQuery.Predicates.Add(predicate);
            return _activeQuery;
        }

        public DataFileQuery Or(PredicateExpression predicateExpression)
        {
            var predicate = new DataFileQueryPredicate
            {
                PredicateExpressions = new List<IDataFileQueryPredicate> { predicateExpression }
            };
            return Or(predicate);
        }

        public DataFileQuery Or(string format, params object[] args)
        {
            var expression = new PredicateExpression(format, args);
            return _activeQuery.Or(expression);
        }

        public DataFileQuery Or(DataFileColumn column, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(column, comparisonOperator, value);
            return _activeQuery.Or(expression);
        }

        public DataFileQuery Or(string columnName, ComparisonOperator comparisonOperator, object value)
        {
            var expression = new PredicateExpression(columnName, comparisonOperator, value);
            return _activeQuery.Or(expression);
        }

        public DataFileQuery Shuffle()
        {
            _activeQuery.Shuffling = true;
            return _activeQuery;
        }

        public DataFileQuery Deshuffle()
        {
            _activeQuery.Shuffling = false;
            return _activeQuery;
        }

        public DataFileQuery ToggleShuffle()
        {
            _activeQuery.Shuffling = !_activeQuery.Shuffling;
            return _activeQuery;
        }

        public List<DataFileQueryPredicate> GetPredicatesByTargetClause(PredicateClauseType clauseType)
        {
            return Predicates.Where(filter => filter.TargetClause.Equals(clauseType)).ToList();
        }

        public DataFileQuery ClearWhereClause()
        {
            _activeQuery.Predicates.RemoveAll(filter => filter.TargetClause.Equals(PredicateClauseType.Where));
            return _activeQuery;
        }

        public DataFileQuery ClearHavingClause()
        {
            _activeQuery.Predicates.RemoveAll(filter => filter.TargetClause.Equals(PredicateClauseType.Having));
            return _activeQuery;
        }

        public DataFileQuery CloneBatch()
        {
            var entryPointQuery = EntryPoint;
            return entryPointQuery.Clone();
        }

        public DataFileQuery Clone()
        {
            var serializable = new
            {
                SelectExpressions,
                UpdateExpressions,
                OrderByExpressions,
                GroupByExpressions,
                QueryFilters = Predicates,
                AlterExpressions
            };
            var clone = CloneObject<DataFileQuery>(serializable);
            clone.Interface = Interface;
            clone.RowLimit = RowLimit;
            clone.SourceFile = SourceFile;
            clone.InsertIntoExpression = CloneObject<InsertIntoExpression>(InsertIntoExpression);
            clone.Shuffling = Shuffling;
            clone.LastPredicateClauseUsed = LastPredicateClauseUsed;
            clone.Mode = Mode;
            if (Next != null)
            {
                clone.Next = Next.Clone();
                clone.Next.EntryPoint = EntryPoint;
            }
            return clone;
        }

        private static T CloneObject<T>(object obj)
        {
            var serialized = JsonConvert.SerializeObject(obj);
            var clone = JsonConvert.DeserializeObject<T>(serialized);
            return clone;
        }
    }
}
