using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Jh.Data.Sql.Replication.DataContracts;

namespace Jh.Data.Sql.Replication.SqlClient
{
    public class ReplicationTask : IReplicationTask
    {
        IReplicationStrategy _replicationStrategy;
        IReplicationArticle _replicationArticle;
        public ReplicationTask(IReplicationStrategy replicationStrategy, IReplicationArticle replicationArticle)
        {
            _replicationStrategy = replicationStrategy;
            _replicationArticle = replicationArticle;
        }
        void IReplicationTask.Run()
        { 
            _replicationStrategy.Replicate(_replicationArticle);
        }
        
        IReplicationStrategy IReplicationTask.Strategy
        {
            get { return _replicationStrategy; }
        }

        IReplicationArticle IReplicationTask.Article
        {
            get { return _replicationArticle; }
        }
    }
}
