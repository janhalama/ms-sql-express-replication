using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.IntegrationTest
{
    internal class TestDatabaseProvider
    {
        string _connectionString;
        public TestDatabaseProvider(string connectionString)
        {
            _connectionString = connectionString;
        }
        public void CreateTestDatabase(string database)
        {
            string commandText = string.Format(@"CREATE DATABASE [{0}]", database);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(commandText, sqlConnection);
                command.ExecuteNonQuery();
            }
        }

        public void DropTestDatabase(string database)
        {
            string commandText = string.Format(@"ALTER DATABASE [{0}] SET single_user WITH ROLLBACK IMMEDIATE
                                                 DROP DATABASE [{0}]", database);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(commandText, sqlConnection);
                command.ExecuteNonQuery();
            }
        }
        public string GenerateUniqueDatabaseName(string namePrefix)
        {
            return namePrefix + "_" + Guid.NewGuid().ToString().Replace('-', '_');
        }
    }
}
