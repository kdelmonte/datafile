using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataFile.Models;
using DataFile.Models.Query;

namespace DataFile
{
    public partial class DataFileInfo
    {
        public string DatabaseFileImportLocation { get; set; }
        public static readonly string ImportFieldDelimeter = "<#fin#>";
        public static readonly string DatabaseImportFileExtension = ".fsimport";
        public static readonly Column DatabaseRecordIdColumn = new Column("___RecordId");
        public static readonly Column DatabaseRecordGroupColumn = new Column("___GroupId");
        public bool DatabaseSessionActive { get; private set; }

        protected void OpenDatabaseSession()
        {
            if (DatabaseSessionActive) return;

            if (!EvaluatedEntirely)
            {
                EvaluateEntirely();
            }

            DatabaseInterface.ImportFile(this);

            DatabaseSessionActive = true;
            if (OnDatabaseSessionOpen != null)
            {
                OnDatabaseSessionOpen();
            }
        }

        protected void CloseDatabaseSession()
        {
            if (!DatabaseSessionActive)
            {
                return;
            }
            DatabaseInterface.DeleteTable(this);
            DatabaseSessionActive = false;
        }

        private SqlDataReader GetDataReader(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (SqlDataReader)SqlSelect(true, query, grouplessRecordsOnly, groupId);
        }

        private DataTable GetDataTable(DatabaseCommand query = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            return (DataTable)SqlSelect(false, query, grouplessRecordsOnly, groupId);
        }

        private object SqlSelect(bool dataReader, DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.Select(dataReader, query);
        }

        protected DataTable GetSqlSchema(DatabaseCommand query = null)
        {
            if (query == null)
            {
                query = CreateDatabaseCommand();
            }
            return DatabaseInterface.GetSchema(query);
        }

        private int DatabaseUpdate(DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var updateQuery = CreateDatabaseCommand(query);
            ApplyGroupFilters(updateQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.ExecuteNonQuery(query);
        }

        private int DatabaseDelete(DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var deleteQuery = CreateDatabaseCommand(query);
            ApplyGroupFilters(deleteQuery, groupId, grouplessRecordsOnly);
            return DatabaseInterface.ExecuteNonQuery(query);
        }

        private void DatabaseQueryToFile(DatabaseCommand query, string targetFilePath,
            string newDelimeter = null, bool grouplessRecordsOnly = false, string groupId = null)
        {
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            DatabaseInterface.QueryToFile(selectQuery, targetFilePath, newDelimeter);
        }

        private void DatabaseQueryToTable(string targetConnectionString, string targetTable, DatabaseCommand query, bool grouplessRecordsOnly = false, string groupId = null)
        {
            CreateGroupPartition(groupId, grouplessRecordsOnly, query);
            var selectQuery = CreateSelectQuery(query);
            ApplyGroupFilters(selectQuery, groupId, grouplessRecordsOnly);
            DatabaseInterface.QueryToTable(targetConnectionString, targetTable, selectQuery);
        }

        private void AlterTableSchema(DatabaseCommand command)
        {
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

        private DatabaseCommand CreateDatabaseCommand()
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
