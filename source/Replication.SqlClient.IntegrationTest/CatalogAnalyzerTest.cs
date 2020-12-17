using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Jh.Data.Sql.Replication.SqlClient.Factories;
using Jh.Data.Sql.Replication.SqlClient.IntegrationTest.TestModels;
using Moq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Jh.Data.Sql.Replication.SqlClient.IntegrationTest
{
    public class CatalogAnalyzerTest
    {
        Mock<ILog> _logMock;
        ICatalogAnalyzer _testedCatalogAnalyzer;
        TestDatabaseProvider _testDatabaseProvider;
        TableSchemaAnalyzer _tableSchemaAnalyzer;
        string _connectionString;

        public CatalogAnalyzerTest()
        {
            _logMock = new Mock<ILog>();
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
            _tableSchemaAnalyzer = new TableSchemaAnalyzer(_connectionString, _logMock.Object, new SqlCommandFactory());
            _testedCatalogAnalyzer = new CatalogAnalyzer(_connectionString, _logMock.Object, new SqlCommandFactory(), _tableSchemaAnalyzer);
        }

        const string CMD_CREATE_TABLE_WITH_PRIMARY_KEY_WITH_IDENTITY = @"USE [{0}]
                            
                            SET ANSI_NULLS ON
                            
                            SET QUOTED_IDENTIFIER ON
                            
                            CREATE TABLE [{1}].[{2}](
	                            [{2}Id] [int] IDENTITY(1,1) NOT NULL,
	                            [Text] [nvarchar](50) NULL,
                             CONSTRAINT [PK_{2}] PRIMARY KEY CLUSTERED 
                            (
	                            [{2}Id] ASC
                            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                            ) ON [PRIMARY]";

        private void CreateTestTable(string createCommand, string databaseName, string schema, string tableName)
        {
            var commandText = string.Format(createCommand, databaseName, schema, tableName);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(commandText, sqlConnection);
                command.ExecuteNonQuery();
            }
        }

        [Fact]
        public void TestListTables()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication");
            const string SCHEMA = "dbo";
            try
            {
                using (var testContext = new TestContext(_connectionString + $";Database={DATABASE_NAME}"))
                {
                    testContext.Standards.Select(s => s.StandardId > 0);
                    testContext.Students.Select(s => s.StudentId > 0);
                    testContext.SaveChanges();
                }
                Table[] tables = _testedCatalogAnalyzer.ListTables(DATABASE_NAME, SCHEMA);
                Assert.Equal(tables.Length, 3);
                Table studentsTable = tables.FirstOrDefault(table => table.Name == "Students");
                Assert.NotNull(studentsTable);
                Assert.Equal(studentsTable.Columns.Length, 7);
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
    }
}
