using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts
{
    public class Table
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public Column[] Columns { get; set; }
    }
}
