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
    /// Replication strategy truncates target database table and 
    /// transfers all records from the source to the target database.
    /// </summary>
    public class TableSnapshotReplicationStrategy : IReplicationStrategy
    {
        string _sourceConnectionString;
        string _targetConnectionString;
        ILog _log;
        public TableSnapshotReplicationStrategy(string sourceConnectionString, string targetConnectionString, ILog log)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
            _log = log;
        }
        void CheckReplicationPrerequisities(IReplicationArticle article, ITable sourceTable, ITable targetTable)
        {
            IReplicationAnalyzer replicationAnalyzer = new ReplicationAnalyzer(_log);
            if (!replicationAnalyzer.AreTableSchemasReplicationCompliant(sourceTable, targetTable))
                throw new ReplicationException("Source and target table are not replication compliant (there are schema differences in those tables)");
        }
        void IReplicationStrategy.Replicate(IReplicationArticle article)
        {
            if (article.ArticleType != DataContracts.Enums.eArticleType.TABLE)
                throw new ArgumentException("Only ArticleType = eArticleType.TABLE supported by this strategy");
            ITableSchemaAnalyzer sourceTableAnalyzer = new TableSchemaAnalyzer(_sourceConnectionString, _log);
            ITable sourceTable = sourceTableAnalyzer.GetTableInfo(article.SourceDatabaseName, article.SourceSchema, article.ArticleName);
            ITableSchemaAnalyzer targetTableAnalyzer = new TableSchemaAnalyzer(_targetConnectionString, _log);
            ITable targetTable = targetTableAnalyzer.GetTableInfo(article.TargetDatabaseName, article.TargetSchema, article.ArticleName);
            CheckReplicationPrerequisities(article, sourceTable, targetTable);
            Replicate(sourceTable, targetTable);
        }
        private void Replicate(ITable sourceTable, ITable targetTable)
        {
            try
            {
                using (SqlConnection sourceDatabaseConnection = new SqlConnection(_sourceConnectionString))
                {
                    sourceDatabaseConnection.Open();
                    SqlCommand command = new SqlCommand($@"USE [{sourceTable.Database}]
                                                           SELECT * FROM {sourceTable.Schema}.{sourceTable.Name}", sourceDatabaseConnection);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        using (SqlConnection targetDatabaseConnection = new SqlConnection(_targetConnectionString))
                        {
                            targetDatabaseConnection.Open();
                            using (SqlTransaction transaction = targetDatabaseConnection.BeginTransaction())
                            {
                                int syncedRows = 0;
                                try
                                {
                                    //unfortunatelly TRUNCATE can not be used for tables with foreign keys without deleting all foreign keys which is not easy to be done see: https://www.mssqltips.com/sqlservertip/3347/drop-and-recreate-all-foreign-key-constraints-in-sql-server/
                                    SqlCommand deleteCommand = new SqlCommand($@"USE [{targetTable.Database}]
                                                                                 --EXEC sp_MSforeachtable @command1=""ALTER TABLE ? NOCHECK CONSTRAINT ALL""
                                                                                 DELETE FROM {targetTable.Schema}.{targetTable.Name}
                                                                                 --EXEC sp_MSforeachtable @command1=""ALTER TABLE ? CHECK CONSTRAINT ALL""");
                                    deleteCommand.Connection = targetDatabaseConnection;
                                    deleteCommand.Transaction = transaction;
                                    deleteCommand.ExecuteNonQuery();
                                    string identityInsertSetup = targetTable.Columns.Any(c => c.IsIdentity) ? $"SET IDENTITY_INSERT {targetTable.Schema}.{targetTable.Name} ON" : "";
                                    while (reader.Read())
                                    {
                                        SqlCommand insertCommand = new SqlCommand("", targetDatabaseConnection, transaction);
                                        string insertCommandText = $@"USE [{targetTable.Database}]
                                                                      {identityInsertSetup} 
                                                                      INSERT INTO {targetTable.Schema}.{targetTable.Name} ( ";
                                        for (int i = 0; i < sourceTable.Columns.Length; i++)
                                            insertCommandText += $"[{sourceTable.Columns[i].Name}] {((i < sourceTable.Columns.Length - 1) ? "," : ") VALUES (")}";
                                        for (int i = 0; i < sourceTable.Columns.Length; i++)
                                        {
                                            string paramName = $"prm{i}";
                                            insertCommandText += $"@{paramName} {((i < sourceTable.Columns.Length - 1) ? "," : ")")}";
                                            insertCommand.Parameters.AddWithValue(paramName, reader[sourceTable.Columns[i].Name]);
                                        }
                                        insertCommand.CommandText = insertCommandText;
                                        if (insertCommand.ExecuteNonQuery() != 1)
                                            throw new ReplicationException("Replication error: Failed to insert row into target database");
                                        syncedRows++;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _log.Error("Replication exception in transaction", ex);
                                    transaction.Rollback();
                                    throw;
                                }
                                transaction.Commit();
                                _log.DebugFormat($"{sourceTable.Name} synced {syncedRows} rows");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _log.Error("Replication exception", ex);
                throw new ReplicationException($"Snapshot replication failed see inner exception | table {sourceTable.Schema}.{sourceTable.Name}", ex);
            }
        }
    }
}

