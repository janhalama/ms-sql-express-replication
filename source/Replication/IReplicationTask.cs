using Jh.Data.Sql.Replication.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication
{
    public interface IReplicationTask
    {
        /// <summary>
        /// Gets the replication article.
        /// </summary>
        /// <value>
        /// The article.
        /// </value>
        IReplicationArticle Article { get; }
        /// <summary>
        /// Gets the replication strategy.
        /// </summary>
        /// <value>
        /// The replication strategy.
        /// </value>
        IReplicationStrategy Strategy { get; }
        /// <summary>
        /// Runs the replication task.
        /// </summary>
        void Run();
    }
}
