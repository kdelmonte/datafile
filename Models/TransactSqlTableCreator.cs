using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace DataFile.Models
{
    public class TransactSqlTableCreator
    {
        public SqlConnection Connection { get; set; }

        public SqlTransaction Transaction { get; set; }

        public string DestinationTableName { get; set; }

        public TransactSqlTableCreator()
        {
        }

        public TransactSqlTableCreator(SqlConnection connection) : this(connection, null)
        {
        }

        public TransactSqlTableCreator(SqlConnection connection, SqlTransaction transaction)
        {
            Connection = connection;
            Transaction = transaction;
        }

        public object Create(DataTable schema)
        {
            return Create(schema, null);
        }

        public object Create(DataTable schema, int numKeys)
        {
            var primaryKeys = new int[numKeys];
            for (var i = 0; i < numKeys; i++)
            {
                primaryKeys[i] = i;
            }
            return Create(schema, primaryKeys);
        }

        public object Create(DataTable schema, int[] primaryKeys)
        {
            var sql = GetCreateSql(DestinationTableName, schema, primaryKeys);

            SqlCommand cmd;
            if (Transaction != null && Transaction.Connection != null)
                cmd = new SqlCommand(sql, Connection, Transaction);
            else
                cmd = new SqlCommand(sql, Connection);

            return cmd.ExecuteNonQuery();
        }

        public object CreateFromDataTable(DataTable table)
        {
            var sql = GetCreateFromDataTableSql(DestinationTableName, table);

            SqlCommand cmd;
            if (Transaction != null && Transaction.Connection != null)
                cmd = new SqlCommand(sql, Connection, Transaction);
            else
                cmd = new SqlCommand(sql, Connection);

            return cmd.ExecuteNonQuery();
        }

        public static string GetCreateSql(string tableName, DataTable schema, int[] primaryKeys)
        {
            var sql = "CREATE TABLE [" + tableName + "] (\n";

            // columns
            foreach (DataRow column in schema.Rows)
            {
                if (!(schema.Columns.Contains("IsHidden") && (bool) column["IsHidden"]))
                {
                    sql += "\t[" + column["ColumnName"] + "] " + SqlGetType(column);

                    if (schema.Columns.Contains("AllowDBNull") && (bool) column["AllowDBNull"] == false)
                        sql += " NOT NULL";

                    sql += ",\n";
                }
            }
            sql = sql.TrimEnd(',', '\n') + "\n";

            // primary keys
            var pk = ", CONSTRAINT PK_" + tableName + " PRIMARY KEY CLUSTERED (";
            var hasKeys = (primaryKeys != null && primaryKeys.Length > 0);
            if (hasKeys)
            {
                // user defined keys
                foreach (var key in primaryKeys)
                {
                    pk += schema.Rows[key]["ColumnName"] + ", ";
                }
            }
            else
            {
                // check schema for keys
                var keys = string.Join(", ", GetPrimaryKeys(schema));
                pk += keys;
                hasKeys = keys.Length > 0;
            }
            pk = pk.TrimEnd(new[] {',', ' ', '\n'}) + ")\n";
            if (hasKeys) sql += pk;

            sql += ")";

            return sql;
        }

        public static string GetCreateFromDataTableSql(string tableName, DataTable table)
        {
            var sql = "CREATE TABLE [" + tableName + "] (\n";
            // columns
            foreach (DataColumn column in table.Columns)
            {
                sql += "[" + column.ColumnName + "] " + SqlGetType(column) + ",\n";
            }
            sql = sql.TrimEnd(new[] {',', '\n'}) + "\n";
            // primary keys
            if (table.PrimaryKey.Length > 0)
            {
                sql += "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED (";
                foreach (var column in table.PrimaryKey)
                {
                    sql += "[" + column.ColumnName + "],";
                }
                sql = sql.TrimEnd(new[] {','}) + "))\n";
            }

            //if not ends with ")"
            if ((table.PrimaryKey.Length == 0) && (!sql.EndsWith(")")))
            {
                sql += ")";
            }

            return sql;
        }

        public static string[] GetPrimaryKeys(DataTable schema)
        {
            var keys = new List<string>();

            foreach (DataRow column in schema.Rows)
            {
                if (schema.Columns.Contains("IsKey") && (bool) column["IsKey"])
                    keys.Add(column["ColumnName"].ToString());
            }

            return keys.ToArray();
        }

        // Return T-SQL data type definition, based on schema definition for a column
        public static string SqlGetType(object type, int columnSize, int numericPrecision, int numericScale)
        {
            switch (type.ToString())
            {
                case "System.String":
                    return "VARCHAR(" +
                           ((columnSize == -1) ? "255" : (columnSize > 8000) ? "MAX" : columnSize.ToString()) + ")";

                case "System.Decimal":
                    if (numericScale > 0)
                        return "FLOAT";
                    else if (numericPrecision > 10)
                        return "BIGINT";
                    else
                        return "INT";

                case "System.Double":
                case "System.Single":
                    return "FLOAT";

                case "System.Int64":
                    return "BIGINT";

                case "System.Int16":
                case "System.Int32":
                    return "INT";

                case "System.DateTime":
                    return "DATETIME";

                case "System.Boolean":
                    return "BIT";

                case "System.Byte":
                    return "TINYINT";

                case "System.Guid":
                    return "UNIQUEIDENTIFIER";

                default:
                    throw new Exception(type + " not implemented.");
            }
        }

        // Overload based on row from schema table
        private static string SqlGetType(DataRow schemaRow)
        {
            return SqlGetType(schemaRow["DataType"],
                              int.Parse(schemaRow["ColumnSize"].ToString()),
                              int.Parse(schemaRow["NumericPrecision"].ToString()),
                              int.Parse(schemaRow["NumericScale"].ToString()));
        }

        // Overload based on DataColumn from DataTable type
        private static string SqlGetType(DataColumn column)
        {
            return SqlGetType(column.DataType, column.MaxLength, 10, 2);
        }
    }
}