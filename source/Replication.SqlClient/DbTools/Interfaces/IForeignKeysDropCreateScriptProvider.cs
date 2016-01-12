using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.Interfaces
{
    public interface IForeignKeysDropCreateScriptProvider
    {
        void GetScripts(out string dropForeignKeyConstraingsSql, out string createForeignKeyConstraingsSql);
    }
}
