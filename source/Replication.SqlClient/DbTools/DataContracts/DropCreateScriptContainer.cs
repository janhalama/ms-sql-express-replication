using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.DbTools.DataContracts
{
    /// <summary>
    /// Container for drop and create sql scripts
    /// The container is immutable
    /// </summary>
    public class DropCreateScriptContainer
    {
        readonly string _createScript;
        readonly string _dropScript;
        public DropCreateScriptContainer(string createScript, string dropScript)
        {
            _createScript = createScript;
            _dropScript = dropScript;
        }
        public string CreateScript { get { return _createScript; } }
        public string DropScript { get { return _dropScript; } }
    }
}
