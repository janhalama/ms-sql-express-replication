using System.Data.SqlClient;

namespace Jh.Data.Sql.Replication.SqlClient.Factories
{
    public interface ISqlCommandFactory
    {
        SqlCommand CreateSqlCommand(string sqlCommandText);
        SqlCommand CreateSqlCommand(string sqlCommandText, SqlConnection sqlConnection);
        SqlCommand CreateSqlCommand(string sqlCommandText, SqlConnection sqlConnection, SqlTransaction sqlTransaction);
    }
}