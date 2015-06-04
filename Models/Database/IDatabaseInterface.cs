using System.Data;
using System.Data.SqlClient;

namespace DataFile.Models.Database
{
    public interface IDatabaseInterface
    {
        string BuildQuery(DatabaseCommand command);
        string BuildSelectClause(DatabaseCommand command);
        string BuildUpdateClause(DatabaseCommand command);
        string BuildInsertIntoClause(DatabaseCommand command);
        string BuildGroupByClause(DatabaseCommand command);
        string BuildOrderByClause(DatabaseCommand command);
        string BuildFilterClause(DatabaseCommand command, FilterClauseType clauseType);

        void ImportFile(DataFileInfo sourceFile);

        void DropTable(DataFileInfo sourceFile);

        SqlDataReader GetDataReader(DatabaseCommand command);

        DataTable GetDataTable(DatabaseCommand command);

        object Select(bool dataReader, DatabaseCommand command);

        DataTable GetSchema(DatabaseCommand command);

        int ExecuteNonQuery(DatabaseCommand command);

        void QueryToFile(DatabaseCommand command, string targetFilePath, string newDelimeter);

        void QueryToTable(string targetConnectionString, string targetTable, DatabaseCommand command);

        DataFileInformation EvaluateEntirely(DataFileInfo sourceFile);
    }
}