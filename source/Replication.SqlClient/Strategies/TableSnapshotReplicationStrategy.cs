using Jh.Data.Sql.Replication.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.Strategies
{
    /// <summary>
    /// Replication strategy replicates tables and transfers all records from the source to the target database
    /// </summary>
    public class TableSnapshotReplicationStrategy : IReplicationStrategy
    {
        string _sourceConnectionString;
        string _targetConnectionString;
        public TableSnapshotReplicationStrategy(string sourceConnectionString, string targetConnectionString)
        {
            _sourceConnectionString = sourceConnectionString;
            _targetConnectionString = targetConnectionString;
        }
        void IReplicationStrategy.Replicate(IReplicationArticle article)
        {
            if (article.ArticleType == DataContracts.Enums.eArticleType.TABLE)
                throw new ArgumentException("Only ArticleType = eArticleType.TABLE supported by this strategy");

        }
    }
}

