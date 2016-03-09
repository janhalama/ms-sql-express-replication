using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Jh.Data.Sql.Replication.SqlClient.Factories;
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
    public class TableValuesLoaderTest
    {
        Mock<ILog> _logMock;
        ITableValuesLoader _tableValuesLoader;
        ITableSchemaAnalyzer _tableSchemaAnalyzer;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public TableValuesLoaderTest()
        {
            _logMock = new Mock<ILog>();
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
            _tableSchemaAnalyzer = new TableSchemaAnalyzer(_connectionString, _logMock.Object, new SqlCommandFactory());
            _tableValuesLoader = new TableValuesLoader(_connectionString, _logMock.Object, new SqlCommandFactory());
        }

        private void CreateTable(string database, string schema, string tableName)
        {
            string commandText = @"USE [{0}]
                            
                            SET ANSI_NULLS ON
                            
                            SET QUOTED_IDENTIFIER ON
                            
                            CREATE TABLE [{1}].[{2}](
	                            [{2}Id] [int] NOT NULL,
	                            [Text] [nvarchar](50) NULL,
                             CONSTRAINT [PK_{2}] PRIMARY KEY CLUSTERED 
                            (
	                            [{2}Id] ASC
                            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                            ) ON [PRIMARY]

                            ";
            commandText = string.Format(commandText, database, schema, tableName);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(commandText, sqlConnection);
                command.ExecuteNonQuery();
            }
        }

        void InsertIntoTable(string database, string schema, string table, int id, string text)
        {
            string insert1Text = @"USE [{0}]
                                INSERT INTO [{1}].[{2}]
                                       ([{2}Id]
                                       ,[Text])
                                VALUES
                                       ({3}
                                       ,'{4}')";
            insert1Text = string.Format(insert1Text, database, schema, table, id, text);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(insert1Text, sqlConnection);
                command.ExecuteNonQuery();
            }
        }

        [Fact]
        public void GetPrimaryKeyMaxValueHappyPathTest()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestValueLoader");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTable(DATABASE_NAME, SCHEMA, TABLE_NAME);
                InsertIntoTable(DATABASE_NAME, SCHEMA, TABLE_NAME, 1, "some text");
                InsertIntoTable(DATABASE_NAME, SCHEMA, TABLE_NAME, 100, "some text");
                InsertIntoTable(DATABASE_NAME, SCHEMA, TABLE_NAME, 5, "some text");
                Table table = _tableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Assert.Equal(100, _tableValuesLoader.GetPrimaryKeyMaxValue(table));
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }

        [Fact]
        public void GetPrimaryKeyMaxValueNoRecordsInTableTest()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestValueLoader");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTable(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _tableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Assert.Equal(-1, _tableValuesLoader.GetPrimaryKeyMaxValue(table));
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
    }
}
