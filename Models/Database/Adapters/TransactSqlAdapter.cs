using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml;
using DataFile.Interfaces;

namespace DataFile.Models.Database.Adapters
{
    public class TransactSqlAdapter : IDataFileDbAdapter
    {
        private const string NullValueLiteral = "NULL";
        static readonly Dictionary<ConjunctionOperator, string> ConjunctionOperatorDictionary = new Dictionary<ConjunctionOperator, string>
            {
                {ConjunctionOperator.And, "AND"},
                {ConjunctionOperator.Or, "OR"}
            };

        static readonly Dictionary<ComparisonOperator, string> ComparisonOperatorDictionary = new Dictionary<ComparisonOperator, string>
            {
                {ComparisonOperator.Equals, "="},
                {ComparisonOperator.GreaterThan, ">"},
                {ComparisonOperator.LessThan, "<"},
                {ComparisonOperator.GreaterThanOrEqualTo, ">="},
                {ComparisonOperator.LessThanOrEqualTo, "<="},
                {ComparisonOperator.NotEqualTo, "<>"}
            };

        static readonly Dictionary<OrderByDirection, string> OrderByDirectionDictionary = new Dictionary<OrderByDirection, string>
            {
                {OrderByDirection.Asc, "ASC"},
                {OrderByDirection.Desc, "DESC"}
            };

        public string QueryBatchSeparator => $"{Environment.NewLine + Environment.NewLine}{"GO"}{Environment.NewLine + Environment.NewLine}";

        public string ConnectionString { get; set; }
        public string FileImportDirectoryPath { get; set; }
        public int CommandTimeout { get; set; }

        private TransactSqlAdapter()
        {
            CommandTimeout = 30;
        }

        public TransactSqlAdapter(string connectionString, string fileImportDirectoryPath):this()
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("The ConnectionString must be set in order to use this operation");
            }
            ConnectionString = connectionString;
            FileImportDirectoryPath = fileImportDirectoryPath;
        }

        public string BuildQuery(DataFileQuery query)
        {
            var builder = new List<string>();
            var limitClause = query.RowLimit == null ? "" : $" TOP {query.RowLimit} ";
            switch (query.Mode)
            {
                case DataFileQueryMode.Alter:
                    return BuildAlterClause(query);
                case DataFileQueryMode.Select:
                    builder.Add($"SELECT{limitClause} {BuildSelectClause(query)}");
                    builder.Add($"FROM [{query.SourceFile.TableName}]");
                    break;
                case DataFileQueryMode.Delete:
                    builder.Add($"DELETE{limitClause} [{query.SourceFile.TableName}]");
                    break;
                case DataFileQueryMode.Update:
                    builder.Add($"UPDATE{limitClause} [{query.SourceFile.TableName}]");
                    builder.Add("SET");
                    builder.Add(BuildUpdateClause(query));
                    break;
                case DataFileQueryMode.Insert:
                    builder.Add($"INSERT INTO [{query.SourceFile.TableName}] {BuildInsertIntoClause(query)}");
                    break;
            }
            var whereClause = BuildPredicateClause(query, PredicateClauseType.Where);
            if (!string.IsNullOrWhiteSpace(whereClause))
            {
                builder.Add("WHERE");
                builder.Add(whereClause);
            }
            var groupByClause = BuildGroupByClause(query);
            if (!string.IsNullOrWhiteSpace(groupByClause))
            {
                builder.Add("GROUP BY");
                builder.Add(groupByClause);
            }
            var havingClause = BuildPredicateClause(query, PredicateClauseType.Having);
            if (!string.IsNullOrWhiteSpace(havingClause))
            {
                builder.Add("HAVING");
                builder.Add(havingClause);
            }
            var orderByClause = BuildOrderByClause(query);
            if (!string.IsNullOrWhiteSpace(orderByClause))
            {
                builder.Add("ORDER BY");
                builder.Add(orderByClause);
            }
            return JoinWithNewLines(builder);
        }

        public string BuildSelectClause(DataFileQuery query)
        {
            var selectClauseBuilder = query.SelectExpressions.Select(exp => BuildExpressionLiteral(exp, true)).ToList();
            if (!selectClauseBuilder.Any()) return null;
            var selectClause = JoinWithCommas(selectClauseBuilder);
            return selectClause;
        }

        public string BuildUpdateClause(DataFileQuery query)
        {
            var updateClauseBuilder = query.UpdateExpressions.Select(BuildUpdateExpressionLiteral).ToList();
            if (!updateClauseBuilder.Any()) return null;
            var updateClause = JoinWithCommas(updateClauseBuilder);
            return updateClause;
        }

        public string BuildInsertIntoClause(DataFileQuery query)
        {
            if (query.InsertIntoExpression == null)
            {
                return null;
            }
            if (!string.IsNullOrWhiteSpace(query.InsertIntoExpression.Literal))
            {
                return query.InsertIntoExpression.Literal;
            }
            var columnClauseBuilder = query.InsertIntoExpression.ColumnExpressions.Select(columnExpression => BuildExpressionLiteral(columnExpression)).ToList();
            var valuesClauseBuilder = query.InsertIntoExpression.Values.Select(BuildValueExpression).ToList();
            var columnClause = JoinWithCommas(columnClauseBuilder);
            var valuesClause = JoinWithCommas(valuesClauseBuilder);
            var insertIntoClause = $"({columnClause}) VALUES ({valuesClause})";
            return insertIntoClause;
        }

        public string BuildGroupByClause(DataFileQuery query)
        {
            var groupByClauseBuilder = query.GroupByExpressions.Select(exp => BuildExpressionLiteral(exp)).ToList();
            if (!groupByClauseBuilder.Any()) return null;
            var groupByClause = JoinWithCommas(groupByClauseBuilder);
            return groupByClause;
        }

        public string BuildOrderByClause(DataFileQuery query)
        {
            if (query.Shuffling)
            {
                return "NEWID()";
            }
            var orderByClauseBuilder = query.OrderByExpressions.Select(BuildOrderByExpressionLiteral).ToList();
            if (!orderByClauseBuilder.Any()) return null;
            var orderByClause = JoinWithCommas(orderByClauseBuilder);
            return orderByClause;
        }

        public string BuildAlterClause(DataFileQuery query)
        {
            var alterTableStatement = $"ALTER TABLE [{query.SourceFile.TableName}]";
            var alterClauseBuilder = new List<string>();
            var addColumnExpressions = query.AlterExpressions.Where(expr => expr.ModificationType == ColumnModificationType.Add).ToList();
            var deleteColumnExpressions = query.AlterExpressions.Where(expr => expr.ModificationType == ColumnModificationType.Delete).ToList();
            var modifyColumnExpressions = query.AlterExpressions.Where(expr => expr.ModificationType == ColumnModificationType.Modify).ToList();

            if (deleteColumnExpressions.Any())
            {
                alterClauseBuilder.Add(alterTableStatement);
                alterClauseBuilder.Add("DROP COLUMN");
                var columnsDeclarationBuilder = new List<string>();
                foreach (var modifyExpression in deleteColumnExpressions)
                {
                    columnsDeclarationBuilder.Add($"{BuildColumnModificationExpressionLiteral(modifyExpression)}");
                }
                alterClauseBuilder.Add(JoinWithCommas(columnsDeclarationBuilder));
            }
            if (modifyColumnExpressions.Any())
            {
                var columnsDeclarationBuilder = new List<string>();
                foreach (var modifyExpression in modifyColumnExpressions)
                {
                    columnsDeclarationBuilder.Add(
                        $"{alterTableStatement} ALTER COLUMN {BuildColumnModificationExpressionLiteral(modifyExpression)}");
                }
                alterClauseBuilder.Add(JoinWithNewLines(columnsDeclarationBuilder));
            }
            if (addColumnExpressions.Any())
            {
                alterClauseBuilder.Add(alterTableStatement);
                alterClauseBuilder.Add("ADD");
                var columnsDeclarationBuilder = new List<string>();
                foreach (var modifyExpression in addColumnExpressions)
                {
                    columnsDeclarationBuilder.Add($"{BuildColumnModificationExpressionLiteral(modifyExpression)}");
                }
                alterClauseBuilder.Add(JoinWithCommas(columnsDeclarationBuilder));
            }
            var alterClause = JoinWithNewLines(alterClauseBuilder);
            return alterClause;
        }

        public string BuildPredicateClause(DataFileQuery query, PredicateClauseType clauseType)
        {
            var predicates = query.GetPredicatesByTargetClause(clauseType);
            if (!predicates.Any()) return null;
            var predicateClauseBuilder = new List<string>();
            foreach (var predicate in predicates)
            {
                predicateClauseBuilder.Add(
                    $"{(predicateClauseBuilder.Any() ? ConjunctionOperatorDictionary[predicate.ConjunctionOperator] + " " : "")}({BuildPredicateLiteral(predicate)})");

            }
            return JoinWithNewLines(predicateClauseBuilder);
        }

        public void ImportFile(DataFileInfo sourceFile)
        {
            var cn = new SqlConnection(ConnectionString);
            DataFileInfo importFile = null;
            try
            {
                var importDirectoryPath = string.IsNullOrEmpty(FileImportDirectoryPath) ? sourceFile.DirectoryName : FileImportDirectoryPath;

                var localImportFilePath = Path.Combine(importDirectoryPath, sourceFile.TableName + DataFileInfo.DatabaseImportFileExtension);

                //Create Temporary Import File
                importFile = !sourceFile.IsFixedWidth ? sourceFile.SaveAs(DataFileFormat.DatabaseImport, localImportFilePath, true) : sourceFile.Copy(localImportFilePath);

                // Create table
                cn.Open();
                var targetTableName = BracketWrap(sourceFile.TableName);
                var sqlBuilder = new List<string>
                {
                    $"CREATE TABLE {targetTableName} ({GetColumnsDeclarationStatement(sourceFile.Layout.Columns)})"
                };
                var cmd = new SqlCommand(JoinWithNewLines(sqlBuilder), cn) { CommandTimeout = CommandTimeout };
                cmd.ExecuteNonQuery();

                using (var bulkCopy = new SqlBulkCopy(cn))
                {
                    bulkCopy.DestinationTableName = targetTableName;
                    using (var dataFileReader = importFile.GetDataReader(true))
                    {
                        bulkCopy.WriteToServer(dataFileReader);
                    }
                }

                sqlBuilder.Clear();
                sqlBuilder.Add($"ALTER TABLE {targetTableName}");
                sqlBuilder.Add("ADD ___RecordId INT IDENTITY (1, 1) NOT NULL, ___GroupId UNIQUEIDENTIFIER NULL");

                cmd = new SqlCommand(JoinWithNewLines(sqlBuilder), cn) { CommandTimeout = CommandTimeout };
                cmd.ExecuteNonQuery();

                importFile.Delete();
            }
            finally
            {

                if (importFile != null && importFile.Exists)
                {
                    importFile.Delete();
                }
                cn.Close();
            }
        }

        public void DropTable(DataFileInfo sourceFile)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlText = $"DROP TABLE [{sourceFile.TableName}]";
                var cmd = new SqlCommand(sqlText, cn) {CommandTimeout = CommandTimeout };

                cn.Open();
                cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        public DataFileInformation Analyze(DataFileInfo sourceFile)
        {
            var fileInfo = new DataFileInformation();
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var targetTableName = BracketWrap(sourceFile.TableName);
                var sqlBuilder = new List<string> {$"SELECT COUNT(*) FROM {targetTableName}"};

                var lengthlessColumns = sourceFile.Layout.Columns.Where(column => !column.LengthSpecified).ToList();

                if (lengthlessColumns.Any())
                {
                    sqlBuilder.Add("SELECT");
                    var lengthQueryBuilder = new List<string>();
 
                    foreach (var lengthlessColumn in lengthlessColumns)
                    {
                        lengthQueryBuilder.Add(string.Format("ISNULL(MAX(DATALENGTH({0})),0) AS {0}", BracketWrap(lengthlessColumn.Name)));
                    }
                    sqlBuilder.Add(JoinWithCommas(lengthQueryBuilder));
                    sqlBuilder.Add($"FROM {targetTableName}");
                }

                var sqlText = JoinWithNewLines(sqlBuilder);
                var cmd = new SqlCommand(sqlText, cn) { CommandTimeout = CommandTimeout };

                var resultSet = new DataSet();
                var dataAdapter = new SqlDataAdapter(cmd);
                cn.Open();
                dataAdapter.Fill(resultSet);

                fileInfo.TotalRecords = Convert.ToInt32(resultSet.Tables[0].Rows[0][0]);

                if (resultSet.Tables.Count <= 1) return fileInfo;
                var columnLengthTable = resultSet.Tables[1];
                var columnLengthRow = columnLengthTable.Rows[0];
                foreach (DataColumn dtColumn in columnLengthTable.Columns)
                {
                    fileInfo.ColumnLengths[dtColumn.ColumnName] = Convert.ToInt32(columnLengthRow[dtColumn.ColumnName]);
                }
                return fileInfo;
            }
            finally
            {
                cn.Close();
            }
        }

        public SqlDataReader GetDataReader(DataFileQuery query)
        {
            return (SqlDataReader)Select(true, query);
        }

        public DataTable GetDataTable(DataFileQuery query)
        {
            return (DataTable)Select(false, query);
        }

        public object Select(bool dataReader, DataFileQuery query)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                cn.Open();
                using (var cmd = new SqlCommand(query.ToQuery(), cn) {CommandTimeout = CommandTimeout })
                {
                    if (dataReader)
                    {
                        return cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    }
                    else
                    {
                        var da = new SqlDataAdapter(cmd);
                        var dt = new DataTable("QueryResults");
                        da.Fill(dt);
                        cn.Close();
                        cn.Dispose();
                        return dt;
                    }
                }
            }
            catch
            {
                cn.Close();
                throw;
            }
            finally
            {
                if (!dataReader)
                {
                    cn.Close();
                }
            }
        }

        public DataTable GetSchema(DataFileQuery query)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                cn.Open();
                var schemaTable = new DataTable();
                var schemaQuery = new DataFileQuery(query.SourceFile, query.Adapter);
                schemaQuery
                    .Select(query.SelectExpressions)
                    .Limit(1)
                    .Where("1 = 2");
                using (var cmd = new SqlCommand(schemaQuery.ToQuery(), cn))
                {
                    var da = new SqlDataAdapter(cmd);
                    da.FillSchema(schemaTable, SchemaType.Source);
                }
                return schemaTable;
            }
            finally
            {
                cn.Close();
            }
        }

        public int ExecuteNonQuery(DataFileQuery query)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var cmd = new SqlCommand(query.ToQuery(), cn) {CommandTimeout = CommandTimeout };
                cn.Open();
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        public void QueryToFile(DataFileQuery query, string targetFilePath, string newDelimiter)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                // TODO: Implement this
            }
            finally
            {
                cn.Close();
                cn.Dispose();
            }
        }

        public void QueryToTable(string targetConnectionString, string targetTableName, DataFileQuery query)
        {
            var sourceTableConnection = new SqlConnection(ConnectionString);
            var targetTableConnection = new SqlConnection(targetConnectionString ?? ConnectionString);
            try
            {
                targetTableConnection.Open();
                bool tableExists;
                const string sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = @TableId";
                using (var cmd = new SqlCommand(sql, targetTableConnection))
                {
                    cmd.Parameters.AddWithValue("@TableId", targetTableName);
                    var count = Convert.ToInt32(cmd.ExecuteScalar());
                    tableExists = count > 0;
                }

                if (!tableExists)
                {
                    sourceTableConnection.Open();
                    var sourceTableSchema = new DataTable();
                    var schemaQuery = new DataFileQuery(query.SourceFile, query.Adapter);
                    schemaQuery
                        .Select(query.SelectExpressions)
                        .Limit(1)
                        .Where("1 = 2");
                    using (var cmd = new SqlCommand(schemaQuery.ToQuery(), sourceTableConnection))
                    {
                        var da = new SqlDataAdapter(cmd);
                        da.FillSchema(sourceTableSchema, SchemaType.Source);
                    }

                    var tableCreator = new TransactSqlTableCreator(targetTableConnection) { DestinationTableName = targetTableName };
                    tableCreator.CreateFromDataTable(sourceTableSchema);
                }

                sourceTableConnection.Open();
                var targetTableSchema = new DataTable();
                var targetSchemaQuery = $"SELECT * FROM [{targetTableName}]";
                using (var cmd = new SqlCommand(targetSchemaQuery, sourceTableConnection))
                {
                    var da = new SqlDataAdapter(cmd);
                    da.FillSchema(targetTableSchema, SchemaType.Source);
                }

                using (var bulkCopy = new SqlBulkCopy(targetTableConnection){BulkCopyTimeout = CommandTimeout})
                {
                    var reader = GetDataReader(query);
                    if (reader.FieldCount != targetTableSchema.Columns.Count)
                    {
                        throw new Exception("The number of columns of the source table do no match the number of columns of the target table");
                    }
                    bulkCopy.DestinationTableName = "[" + targetTableName + "]";
                    bulkCopy.WriteToServer(reader);
                }
            }
            finally
            {
                sourceTableConnection.Close();
                targetTableConnection.Close();
            }
        }


        // Private Methods
        // ========================================

        private static string BuildExpressionLiteral(Expression expression, bool withAlias = false)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }
            var column = expression.Column;
            var literal =
                $"{BracketWrap(column.Name)}{(string.IsNullOrWhiteSpace(column.Alias) || !withAlias ? "" : $" AS {BracketWrap(column.Alias)}")}";
            return literal;
        }

        private static string BuildUpdateExpressionLiteral(UpdateExpression expression)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }

            var updateExpression =
                $"{BuildExpressionLiteral(expression.ColumnExpression)} = {BuildValueExpression(expression.Value)}";
            return updateExpression;
        }

        private static string BuildColumnModificationExpressionLiteral(ColumnModificationExpression expression)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }
            switch (expression.ModificationType)
            {
                    case ColumnModificationType.Add:
                    case ColumnModificationType.Modify:
                    return GetColumnDeclarationStatement(expression.Column);
                    case ColumnModificationType.Delete:
                    return BracketWrap(expression.Column.Name);
            }
            throw new NotImplementedException("The ColumnModificationType is not supported");
        }

        private static string BuildPredicateLiteral(DataFileQueryPredicate predicate)
        {
            var predicateGroupBuilder = new List<string>();
            foreach (var expression in predicate.PredicateExpressions)
            {
                if (expression is DataFileQueryPredicate)
                {
                    var childPredicate = (DataFileQueryPredicate) expression;
                    predicateGroupBuilder.Add(
                        $"{(predicateGroupBuilder.Any() ? ConjunctionOperatorDictionary[childPredicate.ConjunctionOperator] + " " : "")}({BuildPredicateLiteral(childPredicate)})");
                }
                else
                {
                    var predicateExpression = (PredicateExpression) expression;
                    predicateGroupBuilder.Add(
                        $"{(predicateGroupBuilder.Any() ? ConjunctionOperatorDictionary[predicateExpression.ConjunctionOperator] + " " : "")}{BuildPredicateExpressionLiteral(predicateExpression)}");
                }


            }
            return JoinWithSpaces(predicateGroupBuilder);
        }

        private static string BuildPredicateExpressionLiteral(PredicateExpression expression)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }

            var comparisonOperator = ComparisonOperatorDictionary[expression.ComparisonOperator] ?? "=";
            var valueLiteral = BuildValueExpression(expression.Value);
            if (valueLiteral == NullValueLiteral)
            {
                comparisonOperator = "IS";
            }

            var predicate = $"{BuildExpressionLiteral(expression.ColumnExpression)} {comparisonOperator} {valueLiteral}";
            return predicate;
        }

        private static string BuildValueExpression(object value)
        {
            if (value == null || value == DBNull.Value)
            {
                return NullValueLiteral;
            }
            if (value is Expression)
            {
                return BuildExpressionLiteral((Expression) value);
            }

            var type = value.GetType();
            switch (type.FullName)
            {
                case "System.Byte":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                    return $"{value}";
                case "System.Boolean":
                    return $"{((bool) value ? 1 : 0)}";
                case "System.DateTime":
                case "System.Char":
                case "System.String":
                    return $"'{value}'";
                default:
                    throw new Exception("Unsupported value expression type");
            }
        }

        private static string BuildOrderByExpressionLiteral(OrderByExpression expression)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }

            var orderByExpression =
                $"{BuildExpressionLiteral(expression.ColumnExpression)} {OrderByDirectionDictionary[expression.Direction] ?? "ASC"}";
            return orderByExpression;
        }

        private static string GetColumnsDeclarationStatement(IEnumerable<DataFileColumn> columns)
        {
            var columnsCreateStatement = columns.Select(GetColumnDeclarationStatement).ToList();
            return JoinWithCommas(columnsCreateStatement);
        }

        private static string GetColumnDeclarationStatement(DataFileColumn column)
        {
            var length = column.Length > 0 ? column.Length : 1;
            return column.LengthSpecified
                ? $"{WrapWithBrackets(column.Name)} CHAR({length})"
                : $"{WrapWithBrackets(column.Name)} VARCHAR(MAX)";
        }

        private static XmlDocument CreateBcpFormatFile(IEnumerable<DataFileColumn> columns)
        {
            var columnList = columns.ToList();
            const string xsiUri = "http://www.w3.org/2001/XMLSchema-instance";
            var ff = new XmlDocument();
            var dec = ff.CreateXmlDeclaration("1.0", null, null);
            ff.AppendChild(dec);
            var bcpFormat = ff.CreateElement("BCPFORMAT");
            bcpFormat.SetAttribute("xmlns", "http://schemas.microsoft.com/sqlserver/2004/bulkload/format");
            bcpFormat.SetAttribute("xmlns:xsi", xsiUri);
            var record = ff.CreateElement("RECORD");
            var row = ff.CreateElement("ROW");
            for (var x = 0; x < columnList.Count; x++)
            {
                var col = columnList[x];
                var id = (x + 1).ToString();
                var length = col.Length.ToString();
                var column = ff.CreateElement("COLUMN");
                column.SetAttribute("SOURCE", id);
                column.SetAttribute("NAME", col.Name);
                column.SetAttribute("type", xsiUri, "SQLCHAR");
                column.SetAttribute("LENGTH", length);

                var field = ff.CreateElement("FIELD");
                field.SetAttribute("ID", id);
                if (x != columnList.Count - 1)
                {
                    field.SetAttribute("type", xsiUri, "CharFixed");
                    field.SetAttribute("LENGTH", length);
                }
                else
                {
                    field.SetAttribute("type", xsiUri, "CharTerm");
                    field.SetAttribute("TERMINATOR", @"\r\n");
                }

                record.AppendChild(field);
                row.AppendChild(column);
            }
            bcpFormat.AppendChild(record);
            bcpFormat.AppendChild(row);
            ff.AppendChild(bcpFormat);
            return ff;
        }

        private static void CreateBcpFormatFile(IEnumerable<DataFileColumn> columns, string saveToPath)
        {
            var xml = CreateBcpFormatFile(columns);
            xml.Save(saveToPath);
        }

        private static string WrapWithBrackets(string columns)
        {
            if (!columns.Contains(",")) return BracketWrap(columns);
            var split = columns.Split(',');
            for (var i = 0; i < split.Length; i++)
            {
                split[i] = WrapWithBrackets(split[i]);
            }
            return string.Join(",", split);
        }

        private static string BracketWrap(string columnName)
        {
            if (!columnName.EndsWith("]"))
            {
                columnName = "[" + columnName + "]";
            }
            return columnName;
        }

        private static string JoinWithNewLines(IEnumerable<string> builder)
        {
            return string.Join(Environment.NewLine, builder.ToArray());
        }

        private static string JoinWithCommas(IEnumerable<string> builder)
        {
            return string.Join(",", builder.ToArray());
        }

        private static string JoinWithSpaces(IEnumerable<string> builder)
        {
            return string.Join(" ", builder.ToArray());
        }
    }
}
