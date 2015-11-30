using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts
{
    internal class Column : IColumn
    {
        public string Name { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsIdentity { get; set; }
        public System.Data.SqlDbType DataType { get; set; }
    }
}
