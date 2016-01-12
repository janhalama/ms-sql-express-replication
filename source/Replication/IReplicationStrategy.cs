using Jh.Data.Sql.Replication.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication
{
    /// <summary>
    /// Replication strategy interface
    /// </summary>
    public interface IReplicationStrategy
    {
        /// <summary>
        /// Replicates the specified article.
        /// </summary>
        /// <param name="article">The article.</param>
        void Replicate(IReplicationArticle article);
    }
}
