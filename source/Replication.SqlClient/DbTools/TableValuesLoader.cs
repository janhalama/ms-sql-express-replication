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
    internal class TableValuesLoader : ITableValuesLoader
    {
        private string _connectionString;
        private ILog _log;
        public TableValuesLoader(string connectionString, ILog log)
        {
            _connectionString = connectionString;
            _log = log;
        }
        public long GetPrimaryKeyMaxValue(Table table)
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
                {
                    sqlConnection.Open();
                    string commandText = string.Format(@"USE [{0}]
                                                         SELECT MAX([{1}]) FROM [{2}].[{3}]", table.Database, table.Columns.First(c => c.IsPrimaryKey).Name, table.Schema, table.Name);
                    SqlCommand command = new SqlCommand(commandText, sqlConnection);
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
