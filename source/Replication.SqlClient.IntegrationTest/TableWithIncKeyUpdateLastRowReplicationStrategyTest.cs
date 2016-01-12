using Common.Logging;
using EntityFramework.BulkInsert.Extensions;
using Jh.Data.Sql.Replication.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
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
    public class TableWithIncKeyUpdateLastRowReplicationStrategyTest
    {
        Mock<ILog> _logMock;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public TableWithIncKeyUpdateLastRowReplicationStrategyTest()
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

                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyUpdateLastRowReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
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
                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyUpdateLastRowReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
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
        public void ReplicationForeignKeyWithUpdateOnLastRowTest()
        {
            string SOURCE_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Source");
            string TARGET_DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_Target");
            const string SCHEMA = "dbo";
            const int STUDENT_COUNT = 100;
            _testDatabaseProvider.CreateTestDatabase(SOURCE_DATABASE_NAME);
            try
            {
                _testDatabaseProvider.CreateTestDatabase(TARGET_DATABASE_NAME);
                try
                {
                    using (var testContext = new TestContext(_connectionString + $";Database={SOURCE_DATABASE_NAME}"))
                    {
                        var standard = new Standard() { StandardName = "Standard" };
                        testContext.Standards.Add(standard);
                        testContext.SaveChanges();
                        List<Student> students2Add = new List<Student>();
                        for (int i = 0; i < STUDENT_COUNT; i++)
                        {
                            students2Add.Add(new Student()
                            {
                                StandardId = standard.StandardId,
                                Weight = 70 + i,
                                Height = 178 + i,
                                StudentName = $"Student {i}",
                                DateOfBirth = DateTime.Now,
                                Photo = new byte[] { 0, 1, 2, 3 }
                            });
                        }
                        testContext.BulkInsert(students2Add);
                        testContext.SaveChanges();
                    }
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        testContext.Standards.Select(s => s.StandardId > 0);
                        testContext.Students.Select(s => s.StudentId > 0);
                        testContext.SaveChanges();
                    }
                    IReplicationStrategy replicationStrategy = new Strategies.TableWithIncKeyUpdateLastRowReplicationStrategy(_connectionString, _connectionString, _logMock.Object);
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    Student sourceDatabaseLastStudent;
                    using (var testContext = new TestContext(_connectionString + $";Database={SOURCE_DATABASE_NAME}"))
                    {
                        int studentMaxId = testContext.Students.Max(s => s.StudentId);
                        sourceDatabaseLastStudent = testContext.Students.Single(c => c.StudentId == studentMaxId);
                        sourceDatabaseLastStudent.StudentName = "Jan Halama";
                        testContext.SaveChanges();
                    }
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        Assert.Equal(STUDENT_COUNT, testContext.Students.Count());
                        for (int i = 0; i < STUDENT_COUNT - 1; i++)
                        {
                            string expectedStudentName = $"Student {i}";
                            Assert.True(null != testContext.Students.Single(s => s.StudentName == expectedStudentName && s.Weight == 70 + i && s.Height == 178 + i), $"Student {i} not found");
                        }
                        int studentMaxId = testContext.Students.Max(s => s.StudentId);
                        var lastStudent = testContext.Students.Single(c => c.StudentId == studentMaxId);
                        Assert.Equal(sourceDatabaseLastStudent.StudentId, lastStudent.StudentId);
                        Assert.Equal(sourceDatabaseLastStudent.StudentName, lastStudent.StudentName);
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
