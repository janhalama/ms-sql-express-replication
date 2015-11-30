using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer
{
    internal interface ITableSchemaAnalyzer
    {
        IColumn[] GetTableColumns(string catalog, string schema, string table);
        ITable GetTableInfo(string catalog, string schema, string table);
    }
}