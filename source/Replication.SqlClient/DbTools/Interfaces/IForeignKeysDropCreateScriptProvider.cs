using Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    /// <summary>
    /// Interface for generating foreign key constraints drop and create scripts
    /// </summary>
    public interface IForeignKeysDropCreateScriptProvider
    {
        /// <summary>
        /// Generates the drop create scripts for particular database
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>script container</returns>
        DropCreateScriptContainer GenerateScripts(string databaseName);
    }
}
