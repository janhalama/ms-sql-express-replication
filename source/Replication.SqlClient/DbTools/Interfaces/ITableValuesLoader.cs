using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    internal interface ITableValuesLoader
    {
        long GetPrimaryKeyMaxValue(ITable table);
    }
}