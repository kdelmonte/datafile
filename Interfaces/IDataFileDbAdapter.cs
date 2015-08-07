﻿using System.Data;
using System.Data.SqlClient;
using DataFile.Models;
using DataFile.Models.Database;

namespace DataFile.Interfaces
{
    public interface IDataFileDbAdapter
    {
        string QueryBatchSeparator { get; }
        string BuildQuery(DataFileQuery query);
        string BuildSelectClause(DataFileQuery query);
        string BuildUpdateClause(DataFileQuery query);
        string BuildInsertIntoClause(DataFileQuery query);
        string BuildGroupByClause(DataFileQuery query);
        string BuildOrderByClause(DataFileQuery query);
        string BuildPredicateClause(DataFileQuery query, PredicateClauseType clauseType);
        string BuildAlterClause(DataFileQuery query);
        void ImportFile(DataFileInfo sourceFile);
        void DropTable(DataFileInfo sourceFile);
        SqlDataReader GetDataReader(DataFileQuery query);
        DataTable GetDataTable(DataFileQuery query);
        object Select(bool dataReader, DataFileQuery query);
        DataTable GetSchema(DataFileQuery query);
        int ExecuteNonQuery(DataFileQuery query);
        void QueryToFile(DataFileQuery query, string targetFilePath, string newDelimiter);
        void QueryToTable(string targetConnectionString, string targetTableName, DataFileQuery query);
        DataFileInformation Analyze(DataFileInfo sourceFile);
    }
}