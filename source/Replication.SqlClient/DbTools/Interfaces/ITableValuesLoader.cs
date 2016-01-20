using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    /// <summary>
    /// Provides methods for loading some table values
    /// </summary>
    internal interface ITableValuesLoader
    {
        /// <summary>
        /// Gets the primary key maximum value.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        long GetPrimaryKeyMaxValue(Table table);
        /// <summary>
        /// Gets the column maximum value.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        long GetColumnMaxValue(Table table, string columnName);

    }
}