using Common.Logging;
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
using EntityFramework.BulkInsert.Extensions;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Jh.Data.Sql.Replication.SqlClient.Factories;

namespace Jh.Data.Sql.Replication.SqlClient.IntegrationTest
{
    public class TableSnapshotReplicationStrategyTest
    {
        Mock<ILog> _logMock;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;
        IForeignKeysDropCreateScriptProvider _foreignKeysDropCreateScriptProvider;

        public TableSnapshotReplicationStrategyTest()
        {
            _logMock = new Mock<ILog>();
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
            _foreignKeysDropCreateScriptProvider = new ForeignKeysDropCreateScriptProvider(_connectionString, new SqlCommandFactory());
        }

        [Fact]
        public void ReplicationBasicTest()
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
                        student = testContext.Students.Add(new Student()
                        {
                            Standard = standard,
                            Weight = 70,
                            Height = 178,
                            StudentName = "Jan Halama",
                            DateOfBirth = DateTime.Now,
                            Photo = new byte[] { 0, 1, 2, 3 }
                        });
                        testContext.SaveChanges();
                    }
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        testContext.Standards.Select(s => s.StandardId > 0);
                        testContext.Students.Select(s => s.StudentId > 0);
                        testContext.SaveChanges();
                    }
                    IReplicationStrategy replicationStrategy = new Strategies.TableSnapshotReplicationStrategy(_connectionString, _connectionString, _logMock.Object, _foreignKeysDropCreateScriptProvider, new SqlCommandFactory());
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        Assert.True(testContext.Students.Any(s => s.StudentId == student.StudentId && s.StudentName == s.StudentName && s.Standard.StandardId == student.Standard.StandardId), "Target db not as expected after replication");
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
        [Fact]
        public void ReplicationTest100kRows()
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
                    using (var testContext = new TestContext(_connectionString + $";Database={SOURCE_DATABASE_NAME}"))
                    {
                        var standard = new Standard() { StandardName = "Standard" };
                        standard = testContext.Standards.Add(standard);
                        testContext.SaveChanges();
                        List<Student> students2Add = new List<Student>();
                        for (int i = 0; i < 100000; i++)
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
                        var standard = new Standard() { StandardName = "Standard" };
                        standard = testContext.Standards.Add(standard);
                        testContext.SaveChanges();
                        List<Student> students2Add = new List<Student>();
                        for (int i = 0; i < 100000; i++)
                        {
                            students2Add.Add(new Student()
                            {
                                StandardId = standard.StandardId,
                                Weight = 0,
                                Height = 0,
                                StudentName = $"Student old {i}",
                                DateOfBirth = DateTime.Now,
                                Photo = new byte[] { 0, 1, 2, 3 }
                            });
                        }
                        testContext.BulkInsert(students2Add);
                        testContext.SaveChanges();
                    }
                    IReplicationStrategy replicationStrategy = new Strategies.TableSnapshotReplicationStrategy(_connectionString, _connectionString, _logMock.Object, _foreignKeysDropCreateScriptProvider, new SqlCommandFactory());
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        Assert.Equal(100000, testContext.Students.Count());
                        for (int i = 0; i < 100000; i += 99)
                        {
                            string expectedStudentName = $"Student {i}";
                            Assert.True(null != testContext.Students.Single(s => s.StudentName == expectedStudentName && s.Weight == 70 + i && s.Height == 178 + i), $"Student {i} not found");
                        }
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
        [Fact]
        public void ReplicationTestTableWithForeignKeyRepeate()
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
                        student = testContext.Students.Add(new Student()
                        {
                            Standard = standard,
                            Weight = 70,
                            Height = 178,
                            StudentName = "Jan Halama",
                            DateOfBirth = DateTime.Now,
                            Photo = new byte[] { 0, 1, 2, 3 }
                        });
                        testContext.SaveChanges();
                    }
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        testContext.Standards.Select(s => s.StandardId > 0);
                        testContext.Students.Select(s => s.StudentId > 0);
                        testContext.SaveChanges();
                    }
                    IReplicationStrategy replicationStrategy = new Strategies.TableSnapshotReplicationStrategy(_connectionString, _connectionString, _logMock.Object, _foreignKeysDropCreateScriptProvider,new SqlCommandFactory());
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    //repeate; test that foreign keys can be used in tables that are replicated using replication strategy
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Standards", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = "Students", SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    using (var testContext = new TestContext(_connectionString + $";Database={TARGET_DATABASE_NAME}"))
                    {
                        Assert.True(testContext.Students.Any(s => s.StudentId == student.StudentId && s.StudentName == s.StudentName && s.Standard.StandardId == student.Standard.StandardId), "Target db not as expected after replication");
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
