using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer
{
    internal class TableSchemaAnalyzer : ITableSchemaAnalyzer
    {
        private string _connectionString;
        private ILog _log;
        public TableSchemaAnalyzer(string connectionString, ILog log)
        {
            _connectionString = connectionString;
            _log = log;
        }
        IColumn[] ITableSchemaAnalyzer.GetTableColumns(string catalog, string schema, string table)
        {
            try
            {
                string primaryColumnName = GetPrimaryKeyColumnName(catalog, schema, table);
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"USE {0}
                                           SELECT COLUMN_NAME, DATA_TYPE, COLUMNPROPERTY(object_id('{1}.{2}'), COLUMN_NAME, 'IsIdentity') as 'IsIdentity'
                                           FROM [{0}].INFORMATION_SCHEMA.COLUMNS
                                           WHERE TABLE_CATALOG = '{0}' AND TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{2}'";
                    SqlCommand command = new SqlCommand(string.Format(commandText, catalog, schema, table), sqlConnection);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        List<IColumn> res = new List<IColumn>();
                        while (reader.Read())
                        {
                            System.Data.SqlDbType columnType;
                            if (!Enum.TryParse<System.Data.SqlDbType>(reader["DATA_TYPE"].ToString(), true, out columnType))
                                throw new Exception("Failed to parse column type");
                            string columnName = reader["COLUMN_NAME"].ToString();
                            res.Add(new Column() { Name = columnName, DataType = columnType, IsPrimaryKey = columnName == primaryColumnName, IsIdentity = (int)reader["IsIdentity"] == 1 });
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

        string GetPrimaryKeyColumnName(string catalog, string schema, string table)
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"SELECT COLUMN_NAME 
                                           FROM [{0}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                                           WHERE TABLE_CATALOG = @catalog AND TABLE_SCHEMA = @schema AND TABLE_NAME = @table";
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

        ITable ITableSchemaAnalyzer.GetTableInfo(string database, string schema, string table)
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
