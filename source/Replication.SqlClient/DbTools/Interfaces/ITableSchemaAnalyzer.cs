using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    internal interface ITableSchemaAnalyzer
    {
        IColumn[] GetTableColumns(string catalog, string schema, string table);
        ITable GetTableInfo(string catalog, string schema, string table);
    }
}