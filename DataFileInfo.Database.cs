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
        public static readonly string ImportFieldDelimeter = "<#fin#>";
        public static readonly string DatabaseImportFileExtension = ".fsimport";
        public static readonly DataFileColumn DatabaseRecordIdColumn = new DataFileColumn("___RecordId");
        public static readonly DataFileColumn DatabaseRecordGroupColumn = new DataFileColumn("___GroupId");
        public bool DatabaseSessionActive { get; private set; }

        protected void BeginDatabaseSession()
        {
            if (DatabaseSessionActive) return;

            DatabaseInterface.ImportFile(this);

            DatabaseSessionActive = true;

            if (Columns.Any(column => !column.LengthSpecified))
            {
                EvaluateEntirely();
            }
            if (OnDatabaseSessionOpen != null)
            {
                OnDatabaseSessionOpen();
            }
        }

        protected void StopDatabaseSession()
        {
            if (!DatabaseSessionActive)
            {
                return;
            }
            DatabaseInterface.DropTable(this);
            DatabaseSessionActive = false;
        }

        private void EvaluateEntirely()
        {
            BeginDatabaseSession();
            var info = DatabaseInterface.EvaluateEntirely(this);
            TotalRecords = info.TotalRecords;
            if (info.ColumnLengths.Any())
            {
                var columnsWithUpdatedLengths = new List<DataFileColumn>();
                foreach (var columnLengthItem in info.ColumnLengths)
                {
                    var targetColumn = Columns.First(column => column.Name.Equals(columnLengthItem.Key));
                    targetColumn.Length = columnLengthItem.Value;
                    if (targetColumn.Length == 0)
                    {
                        targetColumn.Length = 1;
                    }
                    columnsWithUpdatedLengths.Add(targetColumn);
                }

                var command = CreateDatabaseCommand().Alter(ColumnModificationType.Modify, columnsWithUpdatedLengths);
                DatabaseInterface.ExecuteNonQuery(command);
            }
            EvaluatedEntirely = true;
        }

        public SqlDataReader ToDataReader(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (SqlDataReader)Select(true, query, grouplessRecordsOnly, groupId);
        }

        public DataTable ToDataTable(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (DataTable)Select(false, query, grouplessRecordsOnly, groupId);
        }

        private object Select(bool dataReader, DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.Select(dataReader, query);
        }

        protected DataTable GetSqlSchema(DatabaseCommand query = null)
        {
            BeginDatabaseSession();
            if (query == null)
            {
                query = CreateDatabaseCommand();
            }
            return DatabaseInterface.GetSchema(query);
        }

        public int Update(DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var updateQuery = CreateDatabaseCommand(query);
            ApplyGroupFilters(updateQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.ExecuteNonQuery(query);
        }

        public int DeleteRecords(DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var deleteQuery = CreateDatabaseCommand(query);
            ApplyGroupFilters(deleteQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.ExecuteNonQuery(query);
        }

        public void QueryToFile(DatabaseCommand query, string targetFilePath,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            DatabaseInterface.QueryToFile(selectQuery, targetFilePath, newDelimeter);
        }

        public void QueryToTable(string targetConnectionString, string targetTable, DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            BeginDatabaseSession();
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            DatabaseInterface.QueryToTable(targetConnectionString, targetTable, selectQuery);
        }

        protected void AlterTableSchema(DatabaseCommand command)
        {
            BeginDatabaseSession();
            var alterCommand = CreateDatabaseCommand(command);
            DatabaseInterface.ExecuteNonQuery(alterCommand);
        }

        private void CreateGroupPartition(string groupId, bool grouplessRecordsOnly = false, DatabaseCommand query = null)
        {
            if (string.IsNullOrWhiteSpace(groupId)) return;
            var releaseGroupQuery = CreateDatabaseCommand()
                    .Update(DatabaseRecordGroupColumn, null);
            ApplyGroupFilter(releaseGroupQuery, groupId);
            DatabaseInterface.ExecuteNonQuery(releaseGroupQuery);

            var assignGroupQuery = CreateDatabaseCommand(query)
                .Update(DatabaseRecordGroupColumn, groupId);
            ApplyGrouplessFilter(assignGroupQuery, grouplessRecordsOnly);
            DatabaseInterface.ExecuteNonQuery(assignGroupQuery);
        }

        public DatabaseCommand CreateDatabaseCommand()
        {
            return new DatabaseCommand(DatabaseInterface).From(this);
        }

        private DatabaseCommand CreateDatabaseCommand(DatabaseCommand command)
        {
            return command == null ? CreateDatabaseCommand() : command.Clone().From(this);
        }

        private DatabaseCommand CreateSelectQuery(DatabaseCommand query)
        {
            var selectQuery = CreateDatabaseCommand(query);
            if (!selectQuery.SelectExpressions.Any())
            {
                selectQuery.Select(Columns);
            }
            return selectQuery;
        }

        private static void ApplyGroupFilters(DatabaseCommand query, string groupId, bool grouplessRecordsOnly = true)
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

        private static void ApplyGroupFilter(DatabaseCommand query, string groupId)
        {
            if (groupId != null)
            {
                query.ClearWhereClause().Where(DatabaseRecordGroupColumn, ComparisonOperator.Equals, groupId);
            }
        }

        private static void ApplyGrouplessFilter(DatabaseCommand query, bool grouplessRecordsOnly = true)
        {
            if (grouplessRecordsOnly)
            {
                query.Where(DatabaseRecordGroupColumn, ComparisonOperator.Equals, null);
            }
        }
    }
}
