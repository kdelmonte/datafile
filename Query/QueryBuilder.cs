using System;
using System.Collections.Generic;
using System.Linq;

namespace DataFile.Query
{
    public class QueryBuilder
    {
        private const string WhereClauseIdentifier = "WHERE";
        private const string HavingClauseIdentifier = "HAVING";
        private const string AndOperator = "AND";
        private const string OrOperator = "OR";
        public SqlFlavor Flavor { get; private set; }
        public long? RowLimit { get; private set; }
        public string SelectClause { get; private set; }
        public string FromClause { get; private set; }
        public string DeleteClause { get; private set; }
        public string UpdateClause { get; private set; }
        public string InsertIntoClause { get; private set; }
        public string InsertIntoValuesClause { get; private set; }
        public string OrderByClause { get; private set; }
        public string GroupByClause { get; private set; }

        public string WhereClause
        {
            get
            {
                return BuildFilterClause(WhereClauseIdentifier);
            }
        }

        public string HavingClause
        {
            get
            {
                return BuildFilterClause(HavingClauseIdentifier);
            }
        }

        public string SetClause
        {
            get
            {
                return _setClauses.Any() ? string.Join(Environment.NewLine, _setClauses) : null;
            }
        }


        public QueryBuilder(SqlFlavor flavor)
        {
            Flavor = flavor;
        }

        private string _lastFilterClauseUsed;
        private bool _selecting;
        private bool _deleting;
        private bool _updating;
        private bool _inserting;
        private readonly List<QueryFilter> _queryFilters = new List<QueryFilter>();
        private readonly List<string> _setClauses = new List<string>();

        public QueryBuilder Select(string selectClause, params object[] args)
        {
            if (selectClause == null)
            {
                SelectClause = null;
                return this;
            }
            _selecting = true;
            _deleting = false;
            _updating = false;
            _inserting = false;
            SelectClause = string.Format(selectClause, args);
            return this;
        }
        public QueryBuilder Delete(string deleteClause, params object[] args)
        {
            if (deleteClause == null)
            {
                DeleteClause = null;
                return this;
            }
            _selecting = false;
            _deleting = true;
            _updating = false;
            _inserting = false;
            DeleteClause = string.Format(deleteClause, args);
            return this;
        }
        public QueryBuilder Update(string updateClause, params object[] args)
        {
            if (updateClause == null)
            {
                UpdateClause = null;
                return this;
            }
            _selecting = false;
            _deleting = false;
            _updating = true;
            _inserting = false;
            UpdateClause = string.Format(updateClause, args);
            return this;
        }
        public QueryBuilder InsertInto(string insertIntoClause, params object[] args)
        {
            if (insertIntoClause == null)
            {
                InsertIntoClause = null;
                return this;
            }
            _selecting = false;
            _deleting = false;
            _updating = false;
            _inserting = true;
            InsertIntoClause = string.Format(insertIntoClause, args);
            return this;
        }
        public QueryBuilder From(string fromClause, params object[] args)
        {
            if (fromClause == null)
            {
                FromClause = null;
                return this;
            }
            FromClause = string.Format(fromClause, args);
            return this;
        }
        public QueryBuilder Limit(long? limit)
        {
            RowLimit = limit;
            return this;
        }
        public QueryBuilder Values(string valuesClause, params object[] args)
        {
            if (valuesClause == null)
            {
                InsertIntoValuesClause = null;
                return this;
            }
            InsertIntoValuesClause = string.Format(valuesClause, args);
            return this;
        }
        public QueryBuilder OrderBy(string orderByClause, params object[] args)
        {
            if (orderByClause == null)
            {
                OrderByClause = null;
                return this;
            }
            OrderByClause = string.Format(orderByClause, args);
            return this;
        }
        public QueryBuilder GroupBy(string groupByClause, params object[] args)
        {
            if (groupByClause == null)
            {
                GroupByClause = null;
                return this;
            }
            GroupByClause = string.Format(groupByClause, args);
            return this;
        }
        public QueryBuilder Where(string whereClause, params object[] args)
        {
            _lastFilterClauseUsed = WhereClauseIdentifier;
            _queryFilters.Add(new QueryFilter
            {
                Operator = AndOperator,
                Text = string.Format(whereClause, args),
                TargetClause = WhereClauseIdentifier
            });
            return this;
        }
        public QueryBuilder Having(string havingClause, params object[] args)
        {
            _lastFilterClauseUsed = HavingClauseIdentifier;
            _queryFilters.Add(new QueryFilter
            {
                Operator = AndOperator,
                Text = string.Format(havingClause, args),
                TargetClause = HavingClauseIdentifier
            });
            return this;
        }
        public QueryBuilder And(string clause, params object[] args)
        {
            var filter = new QueryFilter
            {
                Operator = AndOperator,
                Text = string.Format(clause, args),
                TargetClause = _lastFilterClauseUsed
            };
            _queryFilters.Add(filter);
            return this;
        }
        public QueryBuilder Or(string clause, params object[] args)
        {
            var filter = new QueryFilter
            {
                Operator = OrOperator,
                Text = string.Format(clause, args),
                TargetClause = _lastFilterClauseUsed
            };
            _queryFilters.Add(filter);
            return this;
        }
        public QueryBuilder Set(string setClause, params object[] args)
        {
            _setClauses.Add(string.Format(setClause, args));
            return this;
        }
        public override string ToString()
        {
            switch (Flavor)
            {
                    case SqlFlavor.TransactSql:
                    return CreateTransactSqlQuery();
            }
            throw new Exception("Current Flavor not implemented");
        }

        private string CreateTransactSqlQuery()
        {
            var builder = new List<string>();
            var limitClause = RowLimit == null ? "" : string.Format(" TOP {0} ", RowLimit);
            if (_selecting)
            {
                builder.Add(string.Format("SELECT{0}{1}", limitClause, SelectClause));
                builder.Add(string.Format("FROM {0}", FromClause));
            }
            else if (_deleting)
            {
                builder.Add(string.Format("DELETE{0}{1}", limitClause, DeleteClause));
            }
            else if (_updating)
            {
                builder.Add(string.Format("UPDATE{0}{1}", limitClause, UpdateClause));
                builder.Add("SET");
                builder.AddRange(_setClauses);
            }
            else if (_inserting)
            {
                builder.Add(string.Format("INSERT INTO {0}", InsertIntoClause));
                if (!string.IsNullOrWhiteSpace(InsertIntoValuesClause))
                {
                    builder.Add(string.Format("VALUES ({0})", InsertIntoValuesClause));
                }
            }

            var whereClause = BuildFilterClause(WhereClauseIdentifier);
            if (!string.IsNullOrWhiteSpace(whereClause))
            {
                builder.Add("WHERE");
                builder.Add(whereClause);
            }
            if (!string.IsNullOrWhiteSpace(GroupByClause))
            {
                builder.Add("GROUP BY");
                builder.Add(GroupByClause);
            }
            var havingClause = BuildFilterClause(HavingClauseIdentifier);
            if (!string.IsNullOrWhiteSpace(havingClause))
            {
                builder.Add("HAVING");
                builder.Add(havingClause);
            }
            if (!string.IsNullOrWhiteSpace(OrderByClause))
            {
                builder.Add("ORDER BY");
                builder.Add(OrderByClause);
            }

            return string.Join(Environment.NewLine, builder.ToArray());
        }

        private string BuildFilterClause(string targetClause)
        {
            switch (Flavor)
            {
                case SqlFlavor.TransactSql:
                    return BuildTransactSqlFilterClause(targetClause);
            }
            throw new Exception("Current Flavor not implemented");
        }

        private string BuildTransactSqlFilterClause(string targetClause)
        {
            var filters = GetQueryFiltersByTargetClause(targetClause);
            if (filters.Any())
            {
                var filterClauseBuilder = new List<string>();
                foreach (var whereClauseFilter in filters)
                {
                    filterClauseBuilder.Add(filterClauseBuilder.Any()
                        ? string.Format("{0} {1}", whereClauseFilter.Operator, whereClauseFilter.Text)
                        : whereClauseFilter.Text);
                }
                return string.Join(Environment.NewLine, filterClauseBuilder);
            }
            return null;
        }

        private List<QueryFilter> GetQueryFiltersByTargetClause(string targetClause)
        {
            return _queryFilters.Where(filter => filter.TargetClause.Equals(targetClause)).ToList();
        }

        public QueryBuilder ClearWhereClause()
        {
            _queryFilters.RemoveAll(filter => filter.TargetClause.Equals(WhereClauseIdentifier));
            return this;
        }

        public QueryBuilder ClearHavingClause()
        {
            _queryFilters.RemoveAll(filter => filter.TargetClause.Equals(HavingClauseIdentifier));
            return this;
        }

        public QueryBuilder ClearSetClause()
        {
            _setClauses.Clear();
            return this;
        }

        public QueryBuilder Clone()
        {
            var clone = new QueryBuilder(Flavor);
            if (_selecting)
            {
                clone.Select(SelectClause);
            }
            else if (_deleting)
            {
                clone.Delete(DeleteClause);
            }
            else if (_updating)
            {
                clone.Update(UpdateClause);
                foreach (var clause in _setClauses)
                {
                    clone.Set(clause);
                }
            }
            else if (_inserting)
            {
                clone.Delete(InsertIntoClause);
                clone.Values(InsertIntoValuesClause);
            }

            clone.Limit(RowLimit);
            clone.From(FromClause);
            clone.GroupBy(GroupByClause);
            clone.OrderBy(OrderByClause);
            clone._lastFilterClauseUsed = _lastFilterClauseUsed;

            foreach (var queryFilter in GetQueryFiltersByTargetClause(WhereClauseIdentifier))
            {
                switch (queryFilter.Operator)
                {
                    case AndOperator:
                        clone.Where(queryFilter.Text);
                        break;
                    case OrOperator:
                        clone.Or(queryFilter.Text);
                        break;
                }
            }

            foreach (var queryFilter in GetQueryFiltersByTargetClause(HavingClause))
            {
                switch (queryFilter.Operator)
                {
                    case AndOperator:
                        clone.Having(queryFilter.Text);
                        break;
                    case OrOperator:
                        clone.Or(queryFilter.Text);
                        break;
                }
            }
            return clone;
        }
    }
}
