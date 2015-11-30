using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication
{
    public class ReplicationException : Exception
    {
        public ReplicationException(string message): base(message)
        { }
        public ReplicationException(string message, Exception innerException) : base(message, innerException)
        { }
    }
}
