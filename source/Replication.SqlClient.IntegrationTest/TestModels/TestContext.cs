using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.IntegrationTest.TestModels
{
    public class TestContext : DbContext
    {
        public TestContext(string nameOrConnectionString) : base(nameOrConnectionString)
        {

        }

        public DbSet<Student> Students { get; set; }
        public DbSet<Standard> Standards { get; set; }
    }
}
