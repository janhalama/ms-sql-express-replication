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
    public class ForeignKeysDropCreateScriptProviderTest
    {
        TestDatabaseProvider _testDatabaseProvider;
        string _connectionString;

        public ForeignKeysDropCreateScriptProviderTest()
        {
            _connectionString = ConfigurationManager.ConnectionStrings["TestDbServerConnectionString"].ConnectionString;
            _testDatabaseProvider = new TestDatabaseProvider(_connectionString);
        }
        [Fact]
        public void ForeignKeysDropCreateScriptProviderReturnsNotNullNotEmptyScriptsTest()
        {
            string DATABASE_NAME = _testDatabaseProvider.GenerateUniqueDatabaseName("TestReplication_ForeignConstraints");
            _testDatabaseProvider.CreateTestDatabase(DATABASE_NAME);
            IForeignKeysDropCreateScriptProvider foreignKeysDropCreateScriptProvider = new ForeignKeysDropCreateScriptProvider(_connectionString, new SqlCommandFactory());
            try
            {
                Student student;
                using (var testContext = new TestContext(_connectionString + $";Database={DATABASE_NAME}"))
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
                var scriptContainer = foreignKeysDropCreateScriptProvider.GenerateScripts(DATABASE_NAME);
                Assert.NotNull(scriptContainer);
                Assert.True(!string.IsNullOrEmpty(scriptContainer.DropScript));
                Assert.True(!string.IsNullOrEmpty(scriptContainer.CreateScript));
            }
            finally
            {
                _testDatabaseProvider.DropTestDatabase(DATABASE_NAME);
            }
        }
    }
}
