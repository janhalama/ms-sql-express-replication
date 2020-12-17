using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Jh.Data.Sql.Replication.SqlClient.Factories;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools
{
    public class CatalogAnalyzer : ICatalogAnalyzer
    {
        private string _connectionString;
        private ILog _log;
        private ISqlCommandFactory _sqlCommandFactory;
        private ITableSchemaAnalyzer _tableSchemaAnalyzer;
        public CatalogAnalyzer(
            string connectionString, 
            ILog log,
            ISqlCommandFactory sqlCommandFactory,
            ITableSchemaAnalyzer tableSchemaAnalyzer)
        {
            _connectionString = connectionString;
            _log = log;
            _sqlCommandFactory = sqlCommandFactory;
            _tableSchemaAnalyzer = tableSchemaAnalyzer;
        }
        Table[] ICatalogAnalyzer.ListTables(string catalog, string schema)
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = @"USE [{0}]
                                           SELECT TABLE_NAME, TABLE_TYPE
                                           FROM [{0}].INFORMATION_SCHEMA.TABLES
                                           WHERE TABLE_CATALOG = '{0}' AND TABLE_SCHEMA = '{1}'";
                    SqlCommand command = _sqlCommandFactory.CreateSqlCommand(string.Format(commandText, catalog, schema), sqlConnection);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        List<Table> res = new List<Table>();
                        while (reader.Read())
                        {
                            string tableName = reader["TABLE_NAME"].ToString();
                            string tableType = reader["TABLE_TYPE"].ToString();
                            if(tableType == "BASE TABLE")
                            {
                                res.Add(new Table
                                {
                                    Database = catalog,
                                    Schema = schema,
                                    Name = tableName,
                                    Columns = this._tableSchemaAnalyzer.GetTableColumns(catalog, schema, tableName)
                                });
                            }
                        }
                        return res.ToArray();
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error("ListTables exception", ex);
                throw;
            }
        }

    }
}
