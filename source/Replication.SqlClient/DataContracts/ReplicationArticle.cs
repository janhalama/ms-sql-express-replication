using Jh.Data.Sql.Replication.DataContracts.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.DataContracts
{
    public class ReplicationArticle : IReplicationArticle
    {
        public eArticleType ArticleType { get; set; }
        public string ArticleName { get; set; }
        public string SourceDatabaseName { get; set; }
        public string TargetDatabaseName { get; set; }
        public string SourceSchema { get; set; }
        public string TargetSchema { get; set; }

    }
}
