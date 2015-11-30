using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer
{
    internal interface IReplicationAnalyzer
    {
        bool AreTableSchemasReplicationCompliant(ITable sourceTable, ITable targetTable);
    }
}