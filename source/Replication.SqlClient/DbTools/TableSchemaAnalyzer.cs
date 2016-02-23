using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools
{
    public class TableSchemaAnalyzer : ITableSchemaAnalyzer
    {
        private string _connectionString;
        private ILog _log;
        public TableSchemaAnalyzer(string connectionString, ILog log)
        {
            _connectionString = connectionString;
            _log = log;
        }
        Column[] ITableSchemaAnalyzer.GetTableColumns(string catalog, string schema, string table)
        {
            try
            {
                string primaryColumnName = GetPrimaryKeyColumnName(catalog, schema, table);
                string[] foreignKeyColumnNames = GetForeignKeyColumnNames(catalog, schema, table);
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"USE [{0}]
                                           SELECT COLUMN_NAME, DATA_TYPE, COLUMNPROPERTY(object_id('{1}.{2}'), COLUMN_NAME, 'IsIdentity') as 'IsIdentity'
                                           FROM [{0}].INFORMATION_SCHEMA.COLUMNS
                                           WHERE TABLE_CATALOG = '{0}' AND TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{2}'";
                    SqlCommand command = new SqlCommand(string.Format(commandText, catalog, schema, table), sqlConnection);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        List<Column> res = new List<Column>();
                        while (reader.Read())
                        {
                            System.Data.SqlDbType columnType;
                            if (!Enum.TryParse<System.Data.SqlDbType>(reader["DATA_TYPE"].ToString(), true, out columnType))
                                throw new Exception("Failed to parse column type");
                            string columnName = reader["COLUMN_NAME"].ToString();
                            res.Add(new Column() { Name = columnName, DataType = columnType, IsPrimaryKey = columnName == primaryColumnName, IsForeignKey = foreignKeyColumnNames.Contains(columnName), IsIdentity = (int)reader["IsIdentity"] == 1 });
                        }
                        return res.ToArray();
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error("GetTableColumnNames exception", ex);
                throw;
            }
        }

        string[] GetForeignKeyColumnNames(string catalog, string schema, string table)
        {
            try
            {
                List<string> result = new List<string>();
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"SELECT colUsage.COLUMN_NAME 
                                           FROM [{0}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE as colUsage
                                           INNER JOIN [{0}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tabCon on tabCon.CONSTRAINT_NAME = colUsage.CONSTRAINT_NAME AND tabCon.CONSTRAINT_SCHEMA = colUsage.CONSTRAINT_SCHEMA 
                                           WHERE colUsage.TABLE_CATALOG = @catalog AND colUsage.TABLE_SCHEMA = @schema AND colUsage.TABLE_NAME = @table AND tabCon.CONSTRAINT_TYPE = 'FOREIGN KEY'";
                    SqlCommand command = new SqlCommand(string.Format(commandText, catalog), sqlConnection);
                    command.Parameters.AddWithValue("@catalog", catalog);
                    command.Parameters.AddWithValue("@schema", schema);
                    command.Parameters.AddWithValue("@table", table);
                    using (SqlDataReader reader = command.ExecuteReader())
                    { 
                        while (reader.Read())
                        {
                            result.Add(reader["COLUMN_NAME"].ToString());
                        }
                    }
                }
                return result.ToArray();
            }
            catch (Exception ex)
            {
                _log.Error("GetForeignKeyColumnNames exception", ex);
                throw;
            }
        }

        string GetPrimaryKeyColumnName(string catalog, string schema, string table)
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"SELECT colUsage.COLUMN_NAME 
                                           FROM [{0}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE as colUsage
                                           INNER JOIN [{0}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tabCon on tabCon.CONSTRAINT_NAME = colUsage.CONSTRAINT_NAME AND tabCon.CONSTRAINT_SCHEMA = colUsage.CONSTRAINT_SCHEMA 
                                           WHERE colUsage.TABLE_CATALOG = @catalog AND colUsage.TABLE_SCHEMA = @schema AND colUsage.TABLE_NAME = @table AND (tabCon.CONSTRAINT_TYPE  collate sql_latin1_general_cp1_ci_as) = 'PRIMARY KEY'";
                    SqlCommand command = new SqlCommand(string.Format(commandText, catalog), sqlConnection);
                    command.Parameters.AddWithValue("@catalog", catalog);
                    command.Parameters.AddWithValue("@schema", schema);
                    command.Parameters.AddWithValue("@table", table);
                    object value = command.ExecuteScalar();
                    if (value == null)
                        return null;
                    else
                        return value.ToString();
                }
            }
            catch (Exception ex)
            {
                _log.Error("GetPrimaryKeyColumnName exception", ex);
                throw;
            }
            
        }

        Table ITableSchemaAnalyzer.GetTableInfo(string database, string schema, string table)
        {
            return new Table()
            {
                Database = database,
                Schema = schema,
                Name = table,
                Columns = (this as ITableSchemaAnalyzer).GetTableColumns(database, schema, table)
            };
        }
    }
}
