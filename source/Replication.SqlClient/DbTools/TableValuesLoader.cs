﻿using Common.Logging;
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
    internal class TableValuesLoader : ITableValuesLoader
    {
        private string _connectionString;
        private ILog _log;
        private ISqlCommandFactory _sqlCommandFactory;
        public TableValuesLoader(string connectionString, ILog log, ISqlCommandFactory  sqlCommandFactory)
        {
            _connectionString = connectionString;
            _log = log;
            _sqlCommandFactory = sqlCommandFactory;
        }
        public long GetPrimaryKeyMaxValue(Table table)
        {
            Column primaryKeyColumn = table.Columns.Single(c => c.IsPrimaryKey);
            return GetColumnMaxValue(table, primaryKeyColumn.Name);
        }
        public long GetColumnMaxValue(Table table, string columnName)
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = string.Format(@"USE [{0}]
                                                         SELECT MAX([{1}]) FROM [{2}].[{3}]", table.Database, columnName, table.Schema, table.Name);
                    SqlCommand command = _sqlCommandFactory.CreateSqlCommand(commandText, sqlConnection);
                    object res = command.ExecuteScalar();
                    return res is DBNull ? -1 : Convert.ToInt64(res);
                }
            }
            catch (Exception ex)
            {
                _log.Error("GetPrimaryKeyMaxValue exception", ex);
                throw;
            }
        }
    }
}
