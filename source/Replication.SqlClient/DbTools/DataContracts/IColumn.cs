using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts
{
    internal interface IColumn
    {
        string Name { get; set; }
        bool IsPrimaryKey { get; set; }
        bool IsIdentity { get; set; }
        System.Data.SqlDbType DataType { get; set; }
    }
}
