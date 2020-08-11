using Common.Logging;
using Jh.Data.Sql.Replication.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
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
    public class TableWithCompositePrimaryKeyReplicationStrategyTest
    {
        Mock<ILog> _logMock;
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public TableWithCompositePrimaryKeyReplicationStrategyTest()
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
	                        [{2}Id1] [int] NOT NULL,
                            [{2}Id2] [int] NOT NULL,
	                        [Text] [nvarchar](50) NULL,
                            CONSTRAINT [PK_{2}] PRIMARY KEY CLUSTERED 
                        (
	                        [{2}Id1] ASC,
                            [{2}Id2] ASC
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
            for (int rowId = 0; rowId < rowsCount; rowId++)
            {
                InsertRowIntoTable(databaseName, schema, tableName, rowId);
            }
        }

        private void InsertRowIntoTable(string databaseName, string schema, string tableName, int rowId)
        {
            string insertTextTemplate = @"USE [{0}]
                                INSERT INTO [{1}].[{2}]
                                       ([{2}Id1]
                                       ,[{2}Id2]
                                       ,[Text])
                                VALUES
                                       (1
                                       ,{3}
                                       ,'some text {3}')";
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                var insertText = string.Format(insertTextTemplate, databaseName, schema, tableName, rowId);
                SqlCommand command = new SqlCommand(insertText, sqlConnection);
                command.ExecuteNonQuery();
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

                    IReplicationStrategy replicationStrategy = new Strategies.TableWithCompositeIncKeyReplicationStrategy(_connectionString, _connectionString, _logMock.Object, new SqlCommandFactory());
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = TABLE_NAME, SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    Assert.Equal(ROWS_COUNT, TableRowsCount(TARGET_DATABASE_NAME, SCHEMA, TABLE_NAME));

                    InsertRowIntoTable(SOURCE_DATABASE_NAME, SCHEMA, TABLE_NAME, ROWS_COUNT);
                    replicationStrategy.Replicate(new ReplicationArticle() { SourceDatabaseName = SOURCE_DATABASE_NAME, TargetDatabaseName = TARGET_DATABASE_NAME, ArticleName = TABLE_NAME, SourceSchema = SCHEMA, TargetSchema = SCHEMA, ArticleType = DataContracts.Enums.eArticleType.TABLE });
                    Assert.Equal(ROWS_COUNT + 1, TableRowsCount(TARGET_DATABASE_NAME, SCHEMA, TABLE_NAME));
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
                    IReplicationStrategy replicationStrategy = new Strategies.TableWithCompositeIncKeyReplicationStrategy(_connectionString, _connectionString, _logMock.Object, new SqlCommandFactory());
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

    }
}
