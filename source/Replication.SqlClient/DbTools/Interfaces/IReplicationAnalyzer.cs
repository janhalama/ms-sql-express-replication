using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    internal interface IReplicationAnalyzer
    {
        bool AreTableSchemasReplicationCompliant(ITable sourceTable, ITable targetTable);
    }
}