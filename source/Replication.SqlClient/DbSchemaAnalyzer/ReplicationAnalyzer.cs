using Common.Logging;
using Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer
{
    internal class ReplicationAnalyzer : IReplicationAnalyzer
    {
        private ILog _log;
        public ReplicationAnalyzer(ILog log)
        {
            _log = log;
        }
        bool IReplicationAnalyzer.AreTableSchemasReplicationCompliant(ITable sourceTable, ITable targetTable)
        {
            //All columns in source table has to contain target table
            foreach (IColumn sourceColumn in sourceTable.Columns)
            {
                if (!targetTable.Columns.Any(c => c.Name == sourceColumn.Name && c.DataType == sourceColumn.DataType && c.IsPrimaryKey == sourceColumn.IsPrimaryKey))
                    return false;
            }
            return true;
        }
    }
}
