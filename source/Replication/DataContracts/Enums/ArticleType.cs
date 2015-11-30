using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.DataContracts.Enums
{
    /// <summary>
    /// The replication article is the entity to be replicated
    /// </summary>
    public enum eArticleType
    {
        /// <summary>
        /// The database table
        /// </summary>
        TABLE,
        /// <summary>
        /// The database stored procedure
        /// </summary>
        PROCEDURE,
        /// <summary>
        /// The database function
        /// </summary>
        FUNCTION
    }
}
