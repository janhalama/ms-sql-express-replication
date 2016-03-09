using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.Factories
{
    public  class SqlCommandFactory : ISqlCommandFactory
    {
        int _sqlCommandTimeout;
        public SqlCommandFactory(int sqlCommandTimeout = 120)
        {
            _sqlCommandTimeout = sqlCommandTimeout;
        }
        public SqlCommand CreateSqlCommand(string sqlCommandText)
        {
            var result = new SqlCommand(sqlCommandText);
            result.CommandTimeout = _sqlCommandTimeout;
            return result;
        }
        public SqlCommand CreateSqlCommand(string sqlCommandText, SqlConnection sqlConnection)
        {
            var result = new SqlCommand(sqlCommandText, sqlConnection);
            result.CommandTimeout = _sqlCommandTimeout;
            return result;
        }

        public SqlCommand CreateSqlCommand(string sqlCommandText, SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            var result = new SqlCommand(sqlCommandText, sqlConnection, sqlTransaction);
            result.CommandTimeout = _sqlCommandTimeout;
            return result;
        }
    }
}
