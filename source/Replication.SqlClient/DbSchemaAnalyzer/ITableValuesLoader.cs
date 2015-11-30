using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer
{
    internal interface ITableValuesLoader
    {
        long GetPrimaryKeyMaxValue(ITable table);
    }
}