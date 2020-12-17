using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    /// <summary>
    /// Analyzes the database table
    /// </summary>
    public interface ICatalogAnalyzer
    {
        /// <summary>
        ///List catalog tables filtered by schema.
        /// </summary>
        /// <param name="catalog">The catalog.</param>
        /// <param name="schema">The schema.</param>
        /// <returns></returns>
        Table[] ListTables(string catalog, string schema);
    }
}