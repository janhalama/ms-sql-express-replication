using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
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
    public class TableSchemaAnalyzerTest
    {
        Mock<ILog> _logMock;
        ITableSchemaAnalyzer _testedTableSchemaAnalyzer;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public TableSchemaAnalyzerTest()
        {
            _logMock = new Mock<ILog>();
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
            _testedTableSchemaAnalyzer = new TableSchemaAnalyzer(_connectionString, _logMock.Object);
        }

        const string CMD_CREATE_TABLE_WITH_PRIMARY_KEY_WITHOUT_IDENTITY = @"USE [{0}]
                            
                            SET ANSI_NULLS ON
                            
                            SET QUOTED_IDENTIFIER ON
                            
                            CREATE TABLE [{1}].[{2}](
	                            [{2}Id] [int] NOT NULL,
	                            [Text] [nvarchar](50) NULL,
                             CONSTRAINT [PK_{2}] PRIMARY KEY CLUSTERED 
                            (
	                            [{2}Id] ASC
                            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                            ) ON [PRIMARY]";

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

        const string CMD_CREATE_TABLE_NO_PRIMARY_KEY = @"USE [{0}]
                            
                            SET ANSI_NULLS ON
                            
                            SET QUOTED_IDENTIFIER ON
                            
                            CREATE TABLE [{1}].[{2}](
	                            [{2}Id] [int] NOT NULL,
	                            [Text] [nvarchar](50) NULL
                            )

                            ";

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
        public void TestGetTableColumnsMethod()
        {
            const string TESTED_DATABASE = "master";
            const string TESTED_SCHEMA = "dbo";
            const string TESTED_TABLE = "spt_values";
            string[] expectedColumnNames = { "name", "number", "type", "low", "high", "status" };
            Column[] actualResultArray = _testedTableSchemaAnalyzer.GetTableColumns(TESTED_DATABASE, TESTED_SCHEMA, TESTED_TABLE);
            Assert.Equal<int>(expectedColumnNames.Count(), actualResultArray.Count());
            foreach (Column actualColumn in actualResultArray)
                Assert.Contains(actualColumn.Name, expectedColumnNames);
        }

        [Fact]
        public void TestGetTableInfoForTableWithPrimaryKey()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication");
            const string TABLE_NAME = "PrimaryKeyTestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTestTable(CMD_CREATE_TABLE_WITH_PRIMARY_KEY_WITHOUT_IDENTITY, DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, "dbo", TABLE_NAME);
                Assert.Equal(TABLE_NAME, table.Name);
                Assert.Equal(DATABASE_NAME, table.Database);
                Assert.Equal(SCHEMA, table.Schema);
                string expectedPrimaryKeyColumnName = string.Format("{0}Id", TABLE_NAME);
                Assert.True(table.Columns.Count(c => c.IsPrimaryKey && (c.Name == expectedPrimaryKeyColumnName)) == 1, "Primary key column not found");
                Assert.True(table.Columns.First(c => c.Name == expectedPrimaryKeyColumnName).DataType == System.Data.SqlDbType.Int);
                Assert.Equal(2, table.Columns.Count());
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
        [Fact]
        public void TestGetTableInfoForTableWithoutPrimaryKey()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTestTable(CMD_CREATE_TABLE_NO_PRIMARY_KEY, DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, "dbo", TABLE_NAME);
                Assert.Equal(TABLE_NAME, table.Name);
                Assert.Equal(DATABASE_NAME, table.Database);
                Assert.Equal(SCHEMA, table.Schema);
                Assert.True(table.Columns.Count(c => c.IsPrimaryKey) == 0, "Primary key column must not exist");
                Assert.Equal(2, table.Columns.Count());
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
        [Fact]
        public void TestGetTableInfoForTableWithIdentitySeedColumn()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTestTable(CMD_CREATE_TABLE_WITH_PRIMARY_KEY_WITH_IDENTITY, DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Assert.Equal(TABLE_NAME, table.Name);
                Assert.Equal(DATABASE_NAME, table.Database);
                Assert.Equal(SCHEMA, table.Schema);
                Assert.True(table.Columns.Count(c => c.IsIdentity) == 1, "Identity seed column expected");
                Assert.Equal(2, table.Columns.Count());
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
        [Fact]
        public void TestGetTableInfoForTableWithForeignKeyColumn()
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
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, "Students");
                Assert.True(table.Columns.Count(c => c.Name == "StandardId" && c.IsForeignKey) == 1, "Foreign key column expected");
                Assert.True(table.Columns.Count(c => c.IsForeignKey) == 1, "Only one foreign key expected");
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
        [Fact]
        public void TestGetTableInfoForTableWithoutIdentitySeedColumn()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTestTable(CMD_CREATE_TABLE_WITH_PRIMARY_KEY_WITHOUT_IDENTITY, DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Assert.Equal(TABLE_NAME, table.Name);
                Assert.Equal(DATABASE_NAME, table.Database);
                Assert.Equal(SCHEMA, table.Schema);
                Assert.True(table.Columns.Count(c => c.IsIdentity) == 0, "No identity seed column expected");
                Assert.Equal(2, table.Columns.Count());
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
        [Fact]
        public void TestSchemaAnalyzisOnDatabaseWithSpaceCharacterInName()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication ");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            try
            {
                CreateTestTable(CMD_CREATE_TABLE_NO_PRIMARY_KEY, DATABASE_NAME, SCHEMA, TABLE_NAME);
                Table table = _testedTableSchemaAnalyzer.GetTableInfo(DATABASE_NAME, SCHEMA, TABLE_NAME);
                Assert.NotNull(table);
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
    }
}
