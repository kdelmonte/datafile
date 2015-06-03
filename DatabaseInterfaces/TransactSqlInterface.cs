using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml;
using DataFile.Models;
using DataFile.Models.Query;

namespace DataFile.DatabaseInterfaces
{
    public class TransactSqlInterface : IDatabaseInterface
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

        public string ConnectionString { get; set; }
        public string FileImportDirectoryPath { get; set; }
        public int CommandTimeout { get; set; }

        private TransactSqlInterface()
        {
            CommandTimeout = 30;
        }

        public TransactSqlInterface(string connectionString, string fileImportDirectoryPath):this()
        {
            if (string.IsNullOrWhiteSpace(ConnectionString))
            {
                throw new ArgumentException("The ConnectionString must be set in order to use this operation");
            }
            ConnectionString = connectionString;
            FileImportDirectoryPath = fileImportDirectoryPath;
        }

        public string BuildQuery(DatabaseCommand command)
        {
            var builder = new List<string>();
            var limitClause = command.RowLimit == null ? "" : string.Format(" TOP {0} ", command.RowLimit);
            if (command.Selecting)
            {

                builder.Add(string.Format("SELECT{0}{1}", limitClause, BuildSelectClause(command)));
                builder.Add(string.Format("FROM [{0}]", command.SourceFile.UniqueIdentifier));
            }
            else if (command.Deleting)
            {
                builder.Add(string.Format("DELETE{0}[{1}]", limitClause, command.SourceFile.UniqueIdentifier));
            }
            else if (command.Updating)
            {
                builder.Add(string.Format("UPDATE{0}[{1}]", limitClause, command.SourceFile.UniqueIdentifier));
                builder.Add(BuildUpdateClause(command));
            }
            else if (command.Inserting)
            {
                builder.Add(string.Format("INSERT INTO {0}", BuildInsertIntoClause(command)));
            }
            var whereClause = BuildFilterClause(command, FilterClauseType.Where);
            if (!string.IsNullOrWhiteSpace(whereClause))
            {
                builder.Add("WHERE");
                builder.Add(whereClause);
            }
            var groupByClause = BuildGroupByClause(command);
            if (!string.IsNullOrWhiteSpace(groupByClause))
            {
                builder.Add("GROUP BY");
                builder.Add(groupByClause);
            }
            var havingClause = BuildFilterClause(command, FilterClauseType.Having);
            if (!string.IsNullOrWhiteSpace(havingClause))
            {
                builder.Add("HAVING");
                builder.Add(havingClause);
            }
            var orderByClause = BuildOrderByClause(command);
            if (!string.IsNullOrWhiteSpace(orderByClause))
            {
                builder.Add("ORDER BY");
                builder.Add(orderByClause);
            }
            return string.Join(Environment.NewLine, builder.ToArray());
        }

        public string BuildSelectClause(DatabaseCommand command)
        {
            var selectClauseBuilder = command.SelectExpressions.Select(exp => BuildExpressionLiteral(exp, true)).ToList();
            if (!selectClauseBuilder.Any()) return null;
            var selectClause = string.Join(",", selectClauseBuilder);
            return selectClause;
        }

        public string BuildUpdateClause(DatabaseCommand command)
        {
            var updateClauseBuilder = command.UpdateExpressions.Select(BuildUpdateExpressionLiteral).ToList();
            if (!updateClauseBuilder.Any()) return null;
            var updateClause = string.Join(",", updateClauseBuilder);
            return updateClause;
        }

        public string BuildInsertIntoClause(DatabaseCommand command)
        {
            if (command.InsertIntoExpression == null)
            {
                return null;
            }
            if (!string.IsNullOrWhiteSpace(command.InsertIntoExpression.Literal))
            {
                return command.InsertIntoExpression.Literal;
            }
            var columnClauseBuilder = command.InsertIntoExpression.ColumnExpressions.Select(columnExpression => BuildExpressionLiteral(columnExpression)).ToList();
            var valuesClauseBuilder = command.InsertIntoExpression.Values.Select(BuildValueExpression).ToList();
            var columnClause = string.Join(",", columnClauseBuilder);
            var valuesClause = string.Join(",", valuesClauseBuilder);
            var insertIntoClause = string.Format("({0}) VALUES ({1})", columnClause, valuesClause);
            return insertIntoClause;
        }

        public string BuildGroupByClause(DatabaseCommand command)
        {
            var groupByClauseBuilder = command.SelectExpressions.Select(exp => BuildExpressionLiteral(exp)).ToList();
            if (!groupByClauseBuilder.Any()) return null;
            var groupByClause = string.Join(",", groupByClauseBuilder);
            return groupByClause;
        }

        public string BuildOrderByClause(DatabaseCommand command)
        {
            if (command.Shuffling)
            {
                return "NEWID()";
            }
            var orderByClauseBuilder = command.OrderByExpressions.Select(BuildOrderByExpressionLiteral).ToList();
            if (!orderByClauseBuilder.Any()) return null;
            var orderByClause = string.Join(",", orderByClauseBuilder);
            return orderByClause;
        }

        public string BuildFilterClause(DatabaseCommand command, FilterClauseType clauseType)
        {
            var filters = command.GetQueryFiltersByTargetClause(clauseType);
            if (!filters.Any()) return null;
            var filterClauseBuilder = new List<string>();
            foreach (var filterExpression in filters)
            {
                filterClauseBuilder.Add(string.Format("{0}{1}",
                    filterClauseBuilder.Any()
                        ? ConjunctionOperatorDictionary[filterExpression.ConjunctionOperator] + " "
                        : "", BuildFilterExpressionLiteral(filterExpression)));

            }
            return string.Join(Environment.NewLine, filterClauseBuilder);
        }

        public void ImportFile(DataFileInfo sourceFile)
        {
            var cn = new SqlConnection(ConnectionString);
            FileInfo formatFile = null;
            DataFileInfo importFile = null;
            try
            {
                if (!sourceFile.EvaluatedEntirely)
                {
                    sourceFile.EvaluateEntirely();
                }

                var importFilePath = !string.IsNullOrEmpty(FileImportDirectoryPath) ? sourceFile.FullName : FileImportDirectoryPath;

                var localImportFilePath = Path.Combine(importFilePath, sourceFile.UniqueIdentifier + DataFileInfo.DatabaseImportFileExtension);

                //Create Temporary Import File
                importFile = sourceFile.IsFixedWidth ? sourceFile.SaveAs(Format.DatabaseImport, localImportFilePath) : sourceFile.Copy(localImportFilePath);

                var targetTableName = BracketWrap(sourceFile.UniqueIdentifier);
                var sqlBuilder = new List<string>
                {
                    string.Format("CREATE TABLE {0} ({1})", targetTableName,
                        GetColumnDeclarationStatement(sourceFile.Columns)),
                    string.Format("BULK INSERT {0}", targetTableName),
                    string.Format("FROM '{0}'", importFile.FullName),
                    "WITH ("
                };

                if (sourceFile.IsFixedWidth)
                {
                    var formatFilePath = Path.Combine(importFilePath, sourceFile.UniqueIdentifier + ".xml");
                    CreateBcpFormatFile(sourceFile.Columns, formatFilePath);
                    formatFile = new FileInfo(formatFilePath);
                    sqlBuilder.Add(string.Format("FORMATFILE = '{0}',", formatFilePath));
                    sqlBuilder.Add("ROWTERMINATOR = '\\r\\n',");
                }
                else
                {
                    sqlBuilder.Add(string.Format("FIELDTERMINATOR = '{0}',", importFile.FieldDelimeter));
                    sqlBuilder.Add("ROWTERMINATOR = '\\n',");
                }
                sqlBuilder.Add(string.Format("FIRSTROW = {0},", sourceFile.HasColumnHeaders ? 2 : 1));
                sqlBuilder.Add("TABLOCK, KEEPNULLS");
                sqlBuilder.Add(")");

                sqlBuilder.Add(string.Format("ALTER TABLE '{0}'", targetTableName));
                sqlBuilder.Add("ADD ___RecordId INT IDENTITY (1, 1) NOT NULL, ___GroupId UNIQUEIDENTIFIER NULL");

                var sqlText = string.Join(Environment.NewLine, sqlBuilder);
                var cmd = new SqlCommand(sqlText, cn) { CommandType = CommandType.Text, CommandTimeout = CommandTimeout };

                cn.Open();
                cmd.ExecuteNonQuery();
                importFile.Delete();
                if (formatFile != null)
                {
                    formatFile.Delete();
                }
            }
            finally
            {
                if (formatFile != null && formatFile.Exists)
                {
                    formatFile.Delete();
                }
                if (importFile != null && importFile.Exists)
                {
                    importFile.Delete();
                }
                cn.Close();
            }
        }

        public void DeleteTable(DataFileInfo sourceFile)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var sqlText = string.Format("DROP TABLE [{0}]", sourceFile.UniqueIdentifier);
                var cmd = new SqlCommand(sqlText, cn) { CommandType = CommandType.Text, CommandTimeout = CommandTimeout };

                cn.Open();
                cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        public SqlDataReader GetDataReader(DatabaseCommand command)
        {
            return (SqlDataReader)Select(true, command);
        }

        public DataTable GetDataTable(DatabaseCommand command)
        {
            return (DataTable)Select(false, command);
        }

        public object Select(bool dataReader, DatabaseCommand command)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                cn.Open();
                using (var cmd = new SqlCommand(command.ToQuery(), cn) { CommandType = CommandType.Text, CommandTimeout = CommandTimeout })
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

        public DataTable GetSchema(DatabaseCommand command)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                cn.Open();
                var schemaTable = new DataTable();
                var schemaQuery = new DatabaseCommand(command.Interface);
                schemaQuery
                    .Select(command.SelectExpressions)
                    .Limit(1)
                    .From(command.SourceFile)
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

        public int ExecuteNonQuery(DatabaseCommand command)
        {
            var cn = new SqlConnection(ConnectionString);
            try
            {
                var cmd = new SqlCommand(command.ToQuery(), cn) { CommandType = CommandType.Text, CommandTimeout = CommandTimeout };
                cn.Open();
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                cn.Close();
            }
        }

        public void QueryToFile(DatabaseCommand command, string targetFilePath, string newDelimeter)
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

        public void QueryToTable(string targetConnectionString, string targetTable, DatabaseCommand command)
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
                    cmd.Parameters.AddWithValue("@TableId", targetTable);
                    var count = Convert.ToInt32(cmd.ExecuteScalar());
                    tableExists = count > 0;
                }

                if (!tableExists)
                {
                    sourceTableConnection.Open();
                    var sourceTableSchema = new DataTable();
                    var schemaQuery = new DatabaseCommand(command.Interface);
                    schemaQuery
                        .Select(command.SelectExpressions)
                        .Limit(1)
                        .From(command.SourceFile)
                        .Where("1 = 2");
                    using (var cmd = new SqlCommand(schemaQuery.ToQuery(), sourceTableConnection))
                    {
                        var da = new SqlDataAdapter(cmd);
                        da.FillSchema(sourceTableSchema, SchemaType.Source);
                    }

                    var tableCreator = new TransactSqlTableCreator(targetTableConnection) { DestinationTableName = targetTable };
                    tableCreator.CreateFromDataTable(sourceTableSchema);
                }

                using (var bulkCopy = new SqlBulkCopy(targetTableConnection))
                {
                    bulkCopy.DestinationTableName = "[" + targetTable + "]";
                    bulkCopy.WriteToServer(GetDataReader(command));
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
            var literal = string.Format("{0}{1}",
                BracketWrap(column.Name),
                string.IsNullOrWhiteSpace(column.Alias) || !withAlias
                    ? ""
                    : string.Format(" AS {0}", BracketWrap(column.Alias)));
            return literal;
        }

        private static string BuildUpdateExpressionLiteral(UpdateExpression expression)
        {
            if (!string.IsNullOrWhiteSpace(expression.Literal))
            {
                return expression.Literal;
            }

            var updateExpression = string.Format("{0} = {1}",
                BuildExpressionLiteral(expression.ColumnExpression),
                BuildValueExpression(expression.Value)
                );
            return updateExpression;
        }

        private static string BuildFilterExpressionLiteral(FilterExpression expression)
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

            var filterExpression = string.Format("{0} {1} {2}",
                BuildExpressionLiteral(expression.ColumnExpression),
                comparisonOperator,
                valueLiteral
                );
            return filterExpression;
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
            switch (type.Name)
            {
                case "System.Byte":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                    return string.Format("{0}", value);
                case "System.Boolean":
                    return string.Format("{0}", (bool) value ? 1 : 0);
                case "System.DateTime":
                case "System.Char":
                case "System.String":
                    return string.Format("{0}", value);
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

            var orderByExpression = string.Format("{0} {1}",
                BuildExpressionLiteral(expression.ColumnExpression),
                OrderByDirectionDictionary[expression.Direction] ?? "ASC"
                );
            return orderByExpression;
        }

        private static string GetColumnDeclarationStatement(IEnumerable<Column> columns)
        {
            var columnsCreateStatement = new List<string>();
            foreach (var column in columns)
            {
                var length = column.Length > 0 ? column.Length : 1;
                columnsCreateStatement.Add(WrapWithBrackets(column.Name) + " CHAR(" + length + ")");
            }
            return string.Join(",", columnsCreateStatement);
        }

        private static XmlDocument CreateBcpFormatFile(IEnumerable<Column> columns)
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
                var id = (col.Index + 1).ToString();
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

        private static void CreateBcpFormatFile(IEnumerable<Column> columns, string saveToPath)
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
            return String.Join(",", split);
        }

        private static string BracketWrap(string columnName)
        {
            if (!columnName.EndsWith("]"))
            {
                columnName = "[" + columnName + "]";
            }
            return columnName;
        }
    }
}
