using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbSchemaAnalyzer.DataContracts
{
    internal class Table : ITable
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public IColumn[] Columns { get; set; }
    }
}
