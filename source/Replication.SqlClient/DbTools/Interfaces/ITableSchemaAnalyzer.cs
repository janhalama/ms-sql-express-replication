using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    /// <summary>
    /// Analyzes the database table
    /// </summary>
    internal interface ITableSchemaAnalyzer
    {
        /// <summary>
        /// Gets the table columns.
        /// </summary>
        /// <param name="catalog">The catalog.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Column[] GetTableColumns(string catalog, string schema, string table);
        /// <summary>
        /// Gets the table information.
        /// </summary>
        /// <param name="catalog">The catalog.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Table GetTableInfo(string catalog, string schema, string table);
    }
}