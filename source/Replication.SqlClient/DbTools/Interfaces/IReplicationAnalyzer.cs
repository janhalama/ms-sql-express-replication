using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    /// <summary>
    /// Provides methods testing that the database table can be replicated
    /// </summary>
    internal interface IReplicationAnalyzer
    {
        /// <summary>
        /// Are the table schemas replication compliant.
        /// </summary>
        /// <param name="sourceTable">The source table.</param>
        /// <param name="targetTable">The target table.</param>
        /// <returns></returns>
        bool AreTableSchemasReplicationCompliant(Table sourceTable, Table targetTable);
    }
}