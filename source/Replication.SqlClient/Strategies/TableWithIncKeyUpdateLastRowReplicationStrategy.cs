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
    /// Special replication strategy for tables with incremental primary key 
    /// with updates on the last row only in the source table
    /// 
    /// Use case:
    /// Production line software saves tests of the components produced on the line. Test results are saved to MS SQL Express database. 
    /// MS SQL Express database tables are periodically replicated to enterprise SQL server.
    /// Production line software inserts test results to database table and performs update only on the last record (with the highest primary key value) in the table.
    /// </summary>
    public class TableWithIncKeyUpdateLastRowReplicationStrategy : IReplicationStrategy
    {
        string _sourceConnectionString;
        string _targetConnectionString;
        ILog _log;
        ISqlCommandFactory _sqlCommandFactory;
        public TableWithIncKeyUpdateLastRowReplicationStrategy(string sourceConnectionString, string targetConnectionString, ILog log, ISqlCommandFactory sqlCommandFactory)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
            _log = log;
            _sqlCommandFactory = sqlCommandFactory;
        }

        void CheckReplicationPrerequisities(IReplicationArticle article, Table sourceTable, Table targetTable)
        {
            //Replication strategy requires that the table contains primary key column of int data type and that the value of the column is incremented 
            if (!sourceTable.Columns.Any(c => c.IsPrimaryKey &&
                                      (c.DataType == System.Data.SqlDbType.TinyInt ||
                                       c.DataType == System.Data.SqlDbType.SmallInt ||
                                       c.DataType == System.Data.SqlDbType.Int ||
                                       c.DataType == System.Data.SqlDbType.BigInt)))
            {
                if (sourceTable.Columns.SingleOrDefault(c => c.IsForeignKey &&
                                       (c.DataType == System.Data.SqlDbType.TinyInt ||
                                        c.DataType == System.Data.SqlDbType.SmallInt ||
                                        c.DataType == System.Data.SqlDbType.Int ||
                                        c.DataType == System.Data.SqlDbType.BigInt)) == null)
                    throw new ReplicationException($"Table {sourceTable.Name} doesn't contain primary key or single foreign key column or the type of the key column is not TinyInt or SmallInt or Int or BigInt");
            }
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
            Column replicationKeyColumn = sourceTable.Columns.FirstOrDefault(c => c.IsPrimaryKey);
            if (replicationKeyColumn == null) //fallback to sigle foreign key
                replicationKeyColumn = targetTable.Columns.Single(c => c.IsForeignKey);
            long targetDatabasePrimaryKeyMaxValue = tableValueLoader.GetColumnMaxValue(targetTable, replicationKeyColumn.Name);
            try
            {
                using (SqlConnection sourceDatabaseSqlConnection = new SqlConnection(_sourceConnectionString))
                {
                    sourceDatabaseSqlConnection.Open();
                    SqlCommand command = _sqlCommandFactory.CreateSqlCommand(string.Format(@"USE [{0}]
                                                                        SELECT * FROM [{1}].[{2}] WHERE [{3}] >= {4}",
                                                                        sourceTable.Database,
                                                                        sourceTable.Schema,
                                                                        sourceTable.Name,
                                                                        replicationKeyColumn.Name, 
                                                                        targetDatabasePrimaryKeyMaxValue), 
                                                                        sourceDatabaseSqlConnection);
                    _log.Debug($"TableWithIncKeyUpdateLastRowReplicationStrategy.Replicate SELECT CMD:{command.CommandText}");
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
                                    string identityInsertSetup = targetTable.Columns.Any(c => c.IsIdentity) ? $"SET IDENTITY_INSERT {targetTable.Schema}.{targetTable.Name} ON" : "";
                                    if (targetDatabasePrimaryKeyMaxValue != -1 && reader.Read())
                                    {
                                        if(Convert.ToInt64(reader[replicationKeyColumn.Name]) == targetDatabasePrimaryKeyMaxValue)
                                            updateCmd(sourceTable, targetTable, replicationKeyColumn, targetDatabasePrimaryKeyMaxValue, reader, targetDatabaseSqlConnection, transaction);
                                        else
                                        {
                                            _log.Debug($"TableWithIncKeyUpdateLastRowReplicationStrategy.Replicate Last record from target database not found in source database | targetDatabasePrimaryKeyMaxValue = {targetDatabasePrimaryKeyMaxValue} | sourceDatabaseValue = {reader[replicationKeyColumn.Name]}");
                                            insertCmd(sourceTable, targetTable, reader, targetDatabaseSqlConnection, transaction, identityInsertSetup);
                                        }
                                        syncedRows++;
                                    }
                                    while (reader.Read())
                                    {
                                        insertCmd(sourceTable, targetTable, reader, targetDatabaseSqlConnection, transaction, identityInsertSetup);
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
                    throw new ReplicationException($"Replication failed see inner exception | table {sourceTable.Schema}.{sourceTable.Name}", ex);
            }
        }

        private void updateCmd(Table sourceTable, Table targetTable, Column replicationKeyColumn, long targetDatabasePrimaryKeyMaxValue, SqlDataReader reader, SqlConnection targetDatabaseSqlConnection, SqlTransaction transaction)
        {
            SqlCommand updateCommand = _sqlCommandFactory.CreateSqlCommand("", targetDatabaseSqlConnection, transaction);
            string updateCommandText = $@"USE [{targetTable.Database}]
                                          UPDATE [{targetTable.Schema}].[{targetTable.Name}]  SET ";
            for (int i = 0; i < sourceTable.Columns.Length; i++)
            {
                if (sourceTable.Columns[i].IsIdentity) continue;
                string paramName = $"prm{i}";
                updateCommandText += $"{sourceTable.Columns[i].Name} = @{paramName}" + ((i < sourceTable.Columns.Length - 1) ? "," : " ");
                updateCommand.Parameters.AddWithValue(paramName, reader[sourceTable.Columns[i].Name]);
            }
            updateCommandText += string.Format(" WHERE [{0}] = {1}", replicationKeyColumn.Name, targetDatabasePrimaryKeyMaxValue);
            updateCommand.CommandText = updateCommandText;
            if (updateCommand.ExecuteNonQuery() != 1)
                throw new ReplicationException("Replication failed. Failed to update row in target database");
        }

        private void insertCmd(Table sourceTable, Table targetTable, SqlDataReader reader, SqlConnection targetDatabaseSqlConnection, SqlTransaction transaction, string identityInsertSetup)
        {
            SqlCommand insertCommand = _sqlCommandFactory.CreateSqlCommand("", targetDatabaseSqlConnection, transaction);
            string insertCommandText = $@"USE [{targetTable.Database}]
                                                                      {identityInsertSetup}
                                                                      INSERT INTO [{targetTable.Schema}].[{targetTable.Name}] ( ";
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
        }
    }
}
