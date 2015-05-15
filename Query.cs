using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fable
{
    class Query
    {
        private const string AndOperator = "AND";
        private const string OrOperator = "OR";
        public SqlFlavor Flavor { get; private set; }
        public long? Limit { get; private set; }
        public string SelectClause { get; private set; }
        public string FromClause { get; private set; }
        public string DeleteClause { get; private set; }
        public string UpdateClause { get; private set; }
        public string InsertIntoClause { get; private set; }
        public string InsertIntoValuesClause { get; private set; }
        public string OrderByClause { get; private set; }
        public string WhereClause
        {
            get
            {
                return string.Join(" ", _whereClauseBuider);
            }
        }
        public string HavingClause
        {
            get
            {
                return string.Join(" ", _havingClauseBuider);
            }
        }

        public Query(SqlFlavor flavor)
        {
            Flavor = flavor;
        }

        private string _lastFilterClauseUsed;
        private bool _selecting;
        private bool _deleting;
        private bool _updating;
        private bool _inserting;
        private readonly List<QueryFilter> _whereClauseBuider = new List<QueryFilter>();
        private readonly List<QueryFilter> _havingClauseBuider = new List<QueryFilter>();

        public Query Select(string selectClause, params object[] args)
        {
            _selecting = true;
            _deleting = false;
            _updating = false;
            _inserting = false;
            SelectClause = string.Format(selectClause, args);
            return this;
        }
        public Query Delete(string deleteClause, params object[] args)
        {
            _selecting = false;
            _deleting = true;
            _updating = false;
            _inserting = false;
            DeleteClause = string.Format(deleteClause, args);
            return this;
        }
        public Query Update(string updateClause, params object[] args)
        {
            _selecting = false;
            _deleting = false;
            _updating = true;
            _inserting = false;
            UpdateClause = string.Format(updateClause, args);
            return this;
        }
        public Query Insert(string insertIntoClause, params object[] args)
        {
            _selecting = false;
            _deleting = false;
            _updating = false;
            _inserting = true;
            InsertIntoClause = string.Format(insertIntoClause, args);
            return this;
        }
        public Query From(string fromClause, params object[] args)
        {
            FromClause = string.Format(fromClause, args);
            return this;
        }
        public Query Top(long? limit)
        {
            Limit = limit;
            return this;
        }
        public Query Values(string valuesClause, params object[] args)
        {
            InsertIntoValuesClause = string.Format(valuesClause, args);
            return this;
        }
        public Query OrderBy(string orderByClause, params object[] args)
        {
            OrderByClause = string.Format(orderByClause, args);
            return this;
        }
        public Query Where(string whereClause, params object[] args)
        {
            _lastFilterClauseUsed = "WHERE";
            _whereClauseBuider.Add(new QueryFilter
            {
                Operator = AndOperator,
                Text = string.Format(whereClause, args)
            });
            return this;
        }
        public Query Having(string havingClause, params object[] args)
        {
            _lastFilterClauseUsed = "HAVING";
            _havingClauseBuider.Add(new QueryFilter
            {
                Operator = AndOperator,
                Text = string.Format(havingClause, args)
            });
            return this;
        }
        public Query And(string clause, params object[] args)
        {
            switch (_lastFilterClauseUsed)
            {
                case "WHERE":
                    Where(clause, args);
                    break;
                case "HAVING":
                    Having(clause, args);
                    break;
            }
            return this;
        }
        public Query Or(string clause, params object[] args)
        {
            var fragment = new QueryFilter
            {
                Operator = OrOperator,
                Text = string.Format(clause, args)
            };
            switch (_lastFilterClauseUsed)
            {
                case "WHERE":
                    _whereClauseBuider.Add(fragment);
                    break;
                case "HAVING":
                    _havingClauseBuider.Add(fragment);
                    break;
            }
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
            var limitClause = Limit == null ? "" : string.Format("TOP {0}", Limit);
            if (_selecting)
            {
                builder.Add(string.Format("SELECT {0} {1}", limitClause, SelectClause));
                builder.Add(string.Format("FROM {0}", FromClause));
            }
            else if (_deleting)
            {
                builder.Add(string.Format("DELETE {0} FROM {1} {2}", limitClause, FromClause, DeleteClause));
            }
            else if (_updating)
            {
                builder.Add(string.Format("UPDATE {0} {1} {2}", limitClause, FromClause, UpdateClause));
            }
            else if (_inserting)
            {
                builder.Add(string.Format("INSERT INTO {0} {1}", FromClause, InsertIntoClause));
                if (!string.IsNullOrWhiteSpace(InsertIntoValuesClause))
                {
                    builder.Add(string.Format("VALUES ({0})", InsertIntoValuesClause));
                }
            }
        }
    }
}
