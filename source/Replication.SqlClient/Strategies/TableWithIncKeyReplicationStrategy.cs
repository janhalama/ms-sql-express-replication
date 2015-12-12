using Common.Logging;
using Jh.Data.Sql.Replication.DataContracts;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.Strategies
{
    /// <summary>
    /// Replication strategy suitable for tables with incremental primary key 
    /// with no updates on the source table records (only inserts)
    /// 
    /// Use case:
    /// Production line software saves tests of the components produced on the line. Test results are saved to MS SQL Express database. 
    /// MS SQL Express database tables are periodically replicated to enterprise SQL server.
    /// Production line software inserts test results to database table and there are no update on the test result table.
    /// </summary>
    public class TableWithIncKeyReplicationStrategy : IReplicationStrategy
    {
        string _sourceConnectionString;
        string _targetConnectionString;
        ILog _log;
        public TableWithIncKeyReplicationStrategy(string sourceConnectionString, string targetConnectionString, ILog log)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
            _log = log;
        }

        void CheckReplicationPrerequisities(IReplicationArticle article, ITable sourceTable, ITable targetTable)
        {
            //Replication strategy requires that the table contains primary key column of int data type and that the value of the column is incremented 
            if (!sourceTable.Columns.Any(c => c.IsPrimaryKey &&
                                      (c.DataType == System.Data.SqlDbType.TinyInt ||
                                       c.DataType == System.Data.SqlDbType.SmallInt ||
                                       c.DataType == System.Data.SqlDbType.Int ||
                                       c.DataType == System.Data.SqlDbType.BigInt)))
                throw new ReplicationException("Table doesn't contain primary key column or the type of the primary key column is not TinyInt or SmallInt or Int or BigInt");
            //TODO: test that primary key is incremented (IDENTITY SEED is set to true on the table)
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_log);
            if (!replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable))
                throw new ReplicationException("Source and target table are not replication compliant (there are schema differences in those tables)");
        }

        void IReplicationStrategy.Replicate(IReplicationArticle article)
        {
            if (article.ArticleType != DataContracts.Enums.eArticleType.TABLE)
                throw new ReplicationException("Only ArticleType = eArticleType.TABLE supported by this strategy");
            ITableSchemaAnalyzer sourceTableAnalyzer = new TableSchemaAnalyzer(_sourceConnectionString, _log);
            ITable sourceTable = sourceTableAnalyzer.GetTableInfo(article.SourceDatabaseName, article.SourceSchema, article.ArticleName);
            ITableSchemaAnalyzer targetTableAnalyzer = new TableSchemaAnalyzer(_targetConnectionString, _log);
            ITable targetTable = targetTableAnalyzer.GetTableInfo(article.TargetDatabaseName, article.TargetSchema, article.ArticleName);
            CheckReplicationPrerequisities(article, sourceTable, targetTable);
            Replicate(sourceTable, targetTable);
        }

        private void Replicate(ITable sourceTable, ITable targetTable)
        {
            ITableValuesLoader tableValueLoader = new TableValuesLoader(_targetConnectionString, _log);
            long targetDatabasePrimaryKeyMaxValue = tableValueLoader.GetPrimaryKeyMaxValue(targetTable);
            try
            {
                using (SqlConnection sourceDatabaseSqlConnection = new SqlConnection(_sourceConnectionString))
                {
                    sourceDatabaseSqlConnection.Open();
                    SqlCommand command = new SqlCommand(string.Format(@"USE {0}
                                                                        SELECT * FROM {1}.{2} WHERE {3} > {4}",
                                                                        sourceTable.Database,
                                                                        sourceTable.Schema,
                                                                        sourceTable.Name,
                                                                        sourceTable.Columns.First(c => c.IsPrimaryKey).Name, 
                                                                        targetDatabasePrimaryKeyMaxValue), 
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
                                        SqlCommand insertCommand = new SqlCommand("", targetDatabaseSqlConnection, transaction);
                                        string identityInsertSetup = targetTable.Columns.Any(c => c.IsIdentity) ?$"SET IDENTITY_INSERT {targetTable.Schema}.{targetTable.Name} ON":"";
                                        string insertCommandText = string.Format(@"USE {0} 
                                                                                   {3}
                                                                                   INSERT INTO {1}.{2} ( ", targetTable.Database, targetTable.Schema, targetTable.Name, identityInsertSetup);
                                        for (int i = 0; i < sourceTable.Columns.Length; i++)
                                            insertCommandText += string.Format("{0}" + ((i < sourceTable.Columns.Length - 1) ? "," : ") VALUES ("), sourceTable.Columns[i].Name);
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
                _log.Error(string.Format("Replicate exception | table: {0}", sourceTable.Name), ex);
                if (ex is ReplicationException)
                    throw;
                else
                    throw new ReplicationException("Replication failed see inner exception", ex);
            }
        }
    }
}
