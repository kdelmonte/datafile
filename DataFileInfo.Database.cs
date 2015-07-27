using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataFile.Models;
using DataFile.Models.Database;

namespace DataFile
{
    public partial class DataFileInfo
    {
        public string DatabaseFileImportLocation { get; set; }
        
        public static readonly DataFileColumn DatabaseRecordIdColumn = new DataFileColumn("___RecordId");
        public static readonly DataFileColumn DatabaseRecordGroupColumn = new DataFileColumn("___GroupId");
        public bool DatabaseSessionActive { get; set; }

        public void BeginDatabaseSession()
        {
            if (DatabaseSessionActive) return;

            DatabaseAdapter.ImportFile(this);

            DatabaseSessionActive = true;

            if (Layout.Columns.Any(column => !column.LengthSpecified))
            {
                EvaluateEntirely();
            }
            if (OnDatabaseSessionOpen != null)
            {
                OnDatabaseSessionOpen();
            }
        }

        public void StopDatabaseSession()
        {
            if (!DatabaseSessionActive)
            {
                return;
            }
            DatabaseAdapter.DropTable(this);
            DatabaseSessionActive = false;
        }

        public void EvaluateEntirely()
        {
            BeginDatabaseSession();
            var info = DatabaseAdapter.EvaluateEntirely(this);
            TotalRecords = info.TotalRecords;
            if (info.ColumnLengths.Any())
            {
                var columnsWithUpdatedLengths = new List<DataFileColumn>();
                foreach (var columnLengthItem in info.ColumnLengths)
                {
                    var targetColumn = Layout.Columns.First(column => column.Name.Equals(columnLengthItem.Key));
                    targetColumn.Length = columnLengthItem.Value;
                    if (targetColumn.Length == 0)
                    {
                        targetColumn.Length = 1;
                    }
                    columnsWithUpdatedLengths.Add(targetColumn);
                }

                var query = CreateQuery().Alter(ColumnModificationType.Modify, columnsWithUpdatedLengths);
                DatabaseAdapter.ExecuteNonQuery(query);
            }
            EvaluatedEntirely = true;
        }

        public SqlDataReader ToSqlDataReader(DataFileQuery query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (SqlDataReader)Select(true, query, grouplessRecordsOnly, groupId);
        }

        public DataTable ToDataTable(DataFileQuery query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (DataTable)Select(false, query, grouplessRecordsOnly, groupId);
        }

        private object Select(bool dataReader, DataFileQuery query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            var queryToExecute = PreExecuteQuery(selectQuery, grouplessRecordsOnly, groupId);
            return DatabaseAdapter.Select(dataReader, queryToExecute);
        }

        public DataTable GetSqlSchema(DataFileQuery query = null)
        {
            BeginDatabaseSession();
            if (query == null)
            {
                query = CreateQuery();
            }
            var queryToExecute = PreExecuteQuery(query);
            return DatabaseAdapter.GetSchema(queryToExecute);
        }

        public int ExecuteNonQuery(DataFileQuery query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return ExecuteNonQuery(true, query, grouplessRecordsOnly, groupId);
        }

        private int ExecuteNonQuery(bool preExecute, DataFileQuery query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var queryToExecute = CreateQuery(query);
            ApplyGroupFilters(queryToExecute, groupId, grouplessRecordsOnly);
            if (preExecute)
            {
                queryToExecute = PreExecuteQuery(query, grouplessRecordsOnly, groupId);
            }
            return DatabaseAdapter.ExecuteNonQuery(queryToExecute);
        }

        public void QueryToFile(DataFileQuery query, string targetFilePath,
            string newDelimiter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            var queryToExecute = PreExecuteQuery(selectQuery, grouplessRecordsOnly, groupId);
            DatabaseAdapter.QueryToFile(queryToExecute, targetFilePath, newDelimiter);
        }

        public void QueryToTable(string targetConnectionString, string targetTable, DataFileQuery query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            var queryToExecute = PreExecuteQuery(selectQuery, grouplessRecordsOnly, groupId);
            DatabaseAdapter.QueryToTable(targetConnectionString, targetTable, queryToExecute);
        }

        public void CreateGroupPartition(string groupId, bool grouplessRecordsOnly = false, DataFileQuery query = null)
        {
            if (string.IsNullOrWhiteSpace(groupId)) return;
            var releaseGroupQuery = CreateQuery()
                    .Update(DatabaseRecordGroupColumn, null);
            ApplyGroupFilter(releaseGroupQuery, groupId);
            DatabaseAdapter.ExecuteNonQuery(releaseGroupQuery);

            var assignGroupQuery = CreateQuery(query)
                .Update(DatabaseRecordGroupColumn, groupId);
            ApplyGrouplessFilter(assignGroupQuery, grouplessRecordsOnly);
            DatabaseAdapter.ExecuteNonQuery(assignGroupQuery);
        }

        public DataFileQuery CreateQuery()
        {
            return new DataFileQuery(this, DatabaseAdapter);
        }

        private DataFileQuery CreateQuery(DataFileQuery query)
        {
            return query == null ? CreateQuery() : query.Clone().SetSourceFile(this);
        }

        private DataFileQuery CreateSelectQuery(DataFileQuery query)
        {
            var selectQuery = CreateQuery(query);
            if (!selectQuery.SelectExpressions.Any())
            {
                selectQuery.Select(Layout.Columns);
            }
            return selectQuery;
        }

        private static void ApplyGroupFilters(DataFileQuery query, string groupId, bool grouplessRecordsOnly = true)
        {
            if (groupId != null)
            {
                ApplyGroupFilter(query, groupId);
            }
            else if (grouplessRecordsOnly)
            {
                ApplyGrouplessFilter(query);
            }
        }

        private static void ApplyGroupFilter(DataFileQuery query, string groupId)
        {
            if (groupId != null)
            {
                query.ClearWhereClause().Where(DatabaseRecordGroupColumn, ComparisonOperator.Equals, groupId);
            }
        }

        private static void ApplyGrouplessFilter(DataFileQuery query, bool grouplessRecordsOnly = true)
        {
            if (grouplessRecordsOnly)
            {
                query.Where(DatabaseRecordGroupColumn, ComparisonOperator.Equals, null);
            }
        }

        private DataFileQuery PreExecuteQuery(DataFileQuery queryBatch, bool grouplessRecordsOnly = false, string groupId = null)
        {
            var queries = queryBatch.GetQueries();
            var last = queries.Last();
            queries.Remove(last);
            foreach (var query in queries)
            {
                ExecuteNonQuery(false, query, grouplessRecordsOnly, groupId);
            }
            return last;
        }
    }
}
