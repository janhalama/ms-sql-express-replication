using Common.Logging;
using Jh.Data.Sql.Replication.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;
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
    public class TableWithIncKeyReplicationStrategyTest
    {
        Mock<ILog> _logMock;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public TableWithIncKeyReplicationStrategyTest()
        {
            _logMock = new Mock<ILog>();
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
        }

        private void CreateTable(string databaseName, string schema, string tableName)
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
            commandText = string.Format(commandText, databaseName, schema, tableName);
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(commandText, sqlConnection);
                command.ExecuteNonQuery();
            }
        }

        private void InsertIntoTable(string databaseName, string schema, string tableName, int rowsCount)
        {
            string insertTextTemplate = @"USE [{0}]
                                INSERT INTO [{1}].[{2}]
                                       ([{2}Id]
                                       ,[Text])
                                VALUES
                                       ({3}
                                       ,'some text {3}')";
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                for (int i = 0; i < rowsCount; i++)
                {
                    var insertText = string.Format(insertTextTemplate, databaseName, schema, tableName, i);
                    SqlCommand command = new SqlCommand(insertText, sqlConnection);
                    command.ExecuteNonQuery();
                }
            }

        }

        private int TableRowsCount(string databaseName, string schema, string tableName)
        {
            string selectCountTemplate = @"USE [{0}]
                                SELECT COUNT(*) FROM [{1}].[{2}]";
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                var selectCountText = string.Format(selectCountTemplate, databaseName, schema, tableName);
                SqlCommand command = new SqlCommand(selectCountText, sqlConnection);
                return (int)command.ExecuteScalar();
            }
        }

        [Fact]
        public void ReplicationTest()
        {
            string SOURCE_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Source");
            string TARGET_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Target");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            const int ROWS_COUNT = 10;
            _testDatabaseProvider.CreateTestDatabase(SOURCE_DATABASE_NAME);
            try
            {
                _testDatabaseProvider.CreateTestDatabase(TARGET_DATABASE_NAME);
                try
                {
                    CreateTable(SOURCE_DATABASE_NAME, SCHEMA, TABLE_NAME);
                    InsertIntoTable(SOURCE_DATABASE_NAME, SCHEMA, TABLE_NAME, ROWS_COUNT);
                    CreateTable(TARGET_DATABASE_NAME, SCHEMA, TABLE_NAME);

                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = TABLE_NAME, SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    Assert.Equal(ROWS_COUNT, TableRowsCount(TARGET_DATABASE_NAME, SCHEMA, TABLE_NAME));
                }
                finally
                {
                    _testDatabaseProvider.DropTestDatabase(TARGET_DATABASE_NAME);
                }
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(SOURCE_DATABASE_NAME);
            }
        }

        [Fact]
        public void ReplicationWrongArticleTypeTest()
        {
            string SOURCE_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Source");
            string TARGET_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Target");
            const string TABLE_NAME = "TestTable";
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(SOURCE_DATABASE_NAME);
            try
            {
                _testDatabaseProvider.CreateTestDatabase(TARGET_DATABASE_NAME);
                try
                {
                    CreateTable(SOURCE_DATABASE_NAME, SCHEMA, TABLE_NAME);
                    CreateTable(TARGET_DATABASE_NAME, SCHEMA, TABLE_NAME);
                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
                    Assert.Throws<ReplicationException>(() => replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = TABLE_NAME, SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.FUNCTION }));
                }
                finally
                {
                    _testDatabaseProvider.DropTestDatabase(TARGET_DATABASE_NAME);
                }
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(SOURCE_DATABASE_NAME);
            }
        }

        [Fact]
        public void ReplicationForeignKeyTest()
        {
            string SOURCE_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Source");
            string TARGET_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Target");
            const string SCHEMA = "dbo";
            _testDatabaseProvider.CreateTestDatabase(SOURCE_DATABASE_NAME);
            try
            {
                _testDatabaseProvider.CreateTestDatabase(TARGET_DATABASE_NAME);
                try
                {
                    Student student;
                    using (var testContext = new TestContext(_connectionString + $";Database={SOURCE_DATABASE_NAME}"))
                    {
                        var standard = new Standard() { StandardName = "Standard" };
                        testContext.Standards.Add(standard);
                        student = testContext.Students.Add(new Student() {
                            Standard = standard,
                            Weight = 70,
                            Height = 178,
                            StudentName = "Jan Halama",
                            DateOfBirth = DateTime.Now,
                            Photo = new byte[] { 0,1,2,3}
                        });
                        testContext.SaveChanges();
                    }
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        testContext.Standards.Select(s => s.StandardId > 0);
                        testContext.Students.Select(s => s.StudentId > 0);
                        testContext.SaveChanges();
                    }
                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        Assert.True(testContext.Students.Any(s => s.StudentId == student.StudentId && s.StudentName == s.StudentName && s.Standard.StandardId == student.Standard.StandardId),"Target db not as expected after replication");
                    }
                 }
                finally
                {
                    _testDatabaseProvider.DropTestDatabase(TARGET_DATABASE_NAME);
                }
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(SOURCE_DATABASE_NAME);
            }
        }
    }
}
