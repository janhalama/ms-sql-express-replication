using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Jh.Data.Sql.Replication.SqlClient.UnitTest
{
    public class ReplicationAnalyzerTest
    {
        Mock<ILog> _logMock;
        public ReplicationAnalyzerTest()
        {
            _logMock = new Mock<ILog>();
        }
        [Fact]
        public void AreTableSchemasReplicationCompliant_TestDifferentColumnTypes()
        {
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_logMock.Object);
            Table sourceTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int }
                }
            };
            Table targetTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.BigInt }
                }
            };
            Assert.True(replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable) == false, "Columns in source and target table with different type not recognized");
        }
        [Fact]
        public void AreTableSchemasReplicationCompliant_TestNewColumnInSourceDatabase()
        {
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_logMock.Object);
            Table sourceTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column3", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int }
                }
            };
            Table targetTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int }
                }
            };
            Assert.True(replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable) == false, "New column in source table not recognized");
        }
        [Fact]
        public void AreTableSchemasReplicationCompliant_TestHappyPath()
        {
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_logMock.Object);
            Table sourceTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int }
                }
            };
            Table targetTable = new Table()
            {
                Name = "TestTable",
                Columns = new Column[]
                {
                    new Column() {Name = "Column1", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int },
                    new Column() {Name = "Column2", IsPrimaryKey = false, DataType = System.Data.SqlDbType.Int }
                }
            };
            Assert.True(replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable), "Happy path - the result must be true");
        }
    }
}
