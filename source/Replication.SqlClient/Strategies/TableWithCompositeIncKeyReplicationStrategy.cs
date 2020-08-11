using Common.Logging;
using Jh.Data.Sql.Replication.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools;
using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces;
using Jh.Data.Sql.Replication.SqlClient.Factories;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.Strategies
{
    /// <summary>
    /// Replication strategy suitable for tables with incremental composite primary key 
    /// with no updates on the source table records (only inserts)
    /// 
    /// Use case:
    /// Production line software saves tests of the components produced on the line. Test results are saved to MS SQL Express database. 
    /// MS SQL Express database tables are periodically replicated to enterprise SQL server.
    /// Production line software inserts test results to database table and there are no update on the test result table.
    /// </summary>
    public class TableWithCompositeIncKeyReplicationStrategy : IReplicationStrategy
    {
        string _sourceConnectionString;
        string _targetConnectionString;
        ILog _log;
        ISqlCommandFactory _sqlCommandFactory;
        public TableWithCompositeIncKeyReplicationStrategy(string sourceConnectionString, string targetConnectionString, ILog log, ISqlCommandFactory sqlCommandFactory)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
            _log = log;
            _sqlCommandFactory = sqlCommandFactory;
        }

        void CheckReplicationPrerequisities(IReplicationArticle article, Table sourceTable, Table targetTable)
        {
            //Replication strategy requires that the table contains primary key column of int data type and that the value of the column is incremented 
            if (sourceTable.Columns.Count(c => c.IsPrimaryKey &&
                    (c.DataType == System.Data.SqlDbType.TinyInt ||
                    c.DataType == System.Data.SqlDbType.SmallInt ||
                    c.DataType == System.Data.SqlDbType.Int ||
                    c.DataType == System.Data.SqlDbType.BigInt)) == 0)
            {             
              throw new ReplicationException($"Table {sourceTable.Name} doesn't contain primary key column or the type of the key column is not TinyInt or SmallInt or Int or BigInt");
            }
            //TODO: test that primary key is incremented (IDENTITY SEED is set to true on the table)
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_log);
            if (!replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable))
                throw new ReplicationException($"Source and target table {sourceTable.Name} are not replication compliant (there are schema differences in those tables)");
        }

        void IReplicationStrategy.Replicate(IReplicationArticle article)
        {
            if (article.ArticleType != DataContracts.Enums.eArticleType.TABLE)
                throw new ReplicationException("Only ArticleType = eArticleType.TABLE supported by this strategy");
            ITableSchemaAnalyzer sourceTableAnalyzer = new TableSchemaAnalyzer(_sourceConnectionString, _log, _sqlCommandFactory);
            Table sourceTable = sourceTableAnalyzer.GetTableInfo(article.SourceDatabaseName, article.SourceSchema, article.ArticleName);
            ITableSchemaAnalyzer targetTableAnalyzer = new TableSchemaAnalyzer(_targetConnectionString, _log, _sqlCommandFactory);
            Table targetTable = targetTableAnalyzer.GetTableInfo(article.TargetDatabaseName, article.TargetSchema, article.ArticleName);
            CheckReplicationPrerequisities(article, sourceTable, targetTable);
            Replicate(sourceTable, targetTable);
        }

        private void Replicate(Table sourceTable, Table targetTable)
        {
            ITableValuesLoader tableValueLoader = new TableValuesLoader(_targetConnectionString, _log, _sqlCommandFactory);
            Column[] replicationPrimaryKeyColumns = sourceTable.Columns.Where(c => c.IsPrimaryKey).ToArray();
            long[] replicationPrimaryKeyColumnTargetTableMaxValues = replicationPrimaryKeyColumns.Select(c => tableValueLoader.GetColumnMaxValue(targetTable, c.Name)).ToArray();
            List<string> tupleComparisons = new List<string>();
            for (int i = 0; i < replicationPrimaryKeyColumns.Length; i++)
            {
                List<string> singleColumnComparisons = new List<string>();
                for (int j = 0; j < replicationPrimaryKeyColumns.Length; j++)
                {
                    if (i < replicationPrimaryKeyColumns.Length - (j + 1))
                        singleColumnComparisons.Add(string.Format(@"{0} = {1}", replicationPrimaryKeyColumns[j].Name, replicationPrimaryKeyColumnTargetTableMaxValues[j]));
                    else
                    {
                        if (i == replicationPrimaryKeyColumns.Length - (j + 1))
                            singleColumnComparisons.Add(string.Format(@"{0} > {1}", replicationPrimaryKeyColumns[j].Name, replicationPrimaryKeyColumnTargetTableMaxValues[j]));
                        else
                            singleColumnComparisons.Add(string.Format(@"{0} >= {1}", replicationPrimaryKeyColumns[j].Name, 0));
                    }
                }
                tupleComparisons.Add(string.Format(@"({0})", string.Join(" AND ", singleColumnComparisons.ToArray())));
            }
            string where = string.Join(" OR ", tupleComparisons.ToArray());
            try
            {
                using (SqlConnection sourceDatabaseSqlConnection = new SqlConnection(_sourceConnectionString))
                {
                    sourceDatabaseSqlConnection.Open();
                    SqlCommand command = _sqlCommandFactory.CreateSqlCommand(string.Format(@"USE [{0}]
                                                                        SELECT * FROM {1}.{2} WHERE {3}",
                                                                        sourceTable.Database,
                                                                        sourceTable.Schema,
                                                                        sourceTable.Name,
                                                                        where),
                                                                        sourceDatabaseSqlConnection);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        using (SqlConnection targetDatabaseSqlConnection = new SqlConnection(_targetConnectionString))
                        {
                            targetDatabaseSqlConnection.Open();
                            using (SqlTransaction transaction = targetDatabaseSqlConnection.BeginTransaction())
                            {
                                int syncedRows = 0;
                                try
                                {
                                    while (reader.Read())
                                    {
                                        SqlCommand insertCommand = _sqlCommandFactory.CreateSqlCommand("", targetDatabaseSqlConnection, transaction);
                                        string identityInsertSetup = targetTable.Columns.Any(c => c.IsIdentity) ?$"SET IDENTITY_INSERT {targetTable.Schema}.{targetTable.Name} ON":"";
                                        string insertCommandText = string.Format(@"USE [{0}] 
                                                                                   {3}
                                                                                   INSERT INTO [{1}].[{2}] ( ", targetTable.Database, targetTable.Schema, targetTable.Name, identityInsertSetup);
                                        for (int i = 0; i < sourceTable.Columns.Length; i++)
                                            insertCommandText += string.Format("[{0}]" + ((i < sourceTable.Columns.Length - 1) ? "," : ") VALUES ("), sourceTable.Columns[i].Name);
                                        for (int i = 0; i < targetTable.Columns.Length; i++)
                                        {
                                            string paramName = string.Format("prm{0}", i);
                                            insertCommandText += string.Format("@" + paramName + ((i < sourceTable.Columns.Length - 1) ? "," : ")"));
                                            insertCommand.Parameters.AddWithValue(paramName, reader[sourceTable.Columns[i].Name]);
                                        }
                                        insertCommand.CommandText = insertCommandText;
                                        if (insertCommand.ExecuteNonQuery() != 1)
                                            throw new ReplicationException("Replication failed. Unable to insert row into target database.");
                                        syncedRows++;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _log.Error(string.Format("Replicate exception in transaction | table: {0}", sourceTable.Name), ex);
                                    transaction.Rollback();
                                    throw;
                                }
                                transaction.Commit();
                                _log.DebugFormat("{0} synced {1} rows", sourceTable.Name, syncedRows);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("Replication exception | table: {0}", sourceTable.Name), ex);
                if (ex is ReplicationException)
                    throw;
                else
                    throw new ReplicationException($"Replication failed see inner exception | table {sourceTable.Schema}.{sourceTable.Name}", ex);
            }
        }
    }
}
