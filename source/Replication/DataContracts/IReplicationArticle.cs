using Jh.Data.Sql.Replication.DataContracts.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.DataContracts
{
    /// <summary>
    /// Replication article is the database entity to be replicated
    /// </summary>
    public interface IReplicationArticle
    {
        /// <summary>
        /// Gets or sets the type of the article.
        /// </summary>
        /// <value>
        /// The type of the article.
        /// </value>
        eArticleType ArticleType { get; set; }
        /// <summary>
        /// Gets or sets the name of the article. 
        /// Article name is typicaly table name or stored procedure name.
        /// </summary>
        /// <value>
        /// The name of the article.
        /// </value>
        string ArticleName { get; set; }
        /// <summary>
        /// Gets or sets the name of the source database.
        /// </summary>
        /// <value>
        /// The name of the source database.
        /// </value>
        string SourceDatabaseName { get; set; }
        /// <summary>
        /// Gets or sets the name of the target database.
        /// </summary>
        /// <value>
        /// The name of the target database.
        /// </value>
        string TargetDatabaseName { get; set; }
        /// <summary>
        /// Gets or sets the source database  schema.
        /// </summary>
        /// <value>
        /// The source database schema.
        /// </value>
        string SourceSchema { get; set; }
        /// <summary>
        /// Gets or sets the target databse schema.
        /// </summary>
        /// <value>
        /// The target databse schema.
        /// </value>
        string TargetSchema { get; set; }

    }
}
