using System;
using System.Collections.Generic;
using System.Data.Entity.ModelConfiguration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jh.Data.Sql.Replication.SqlClient.IntegrationTest.TestModels.Mapping
{
    public class StudentMap : EntityTypeConfiguration<Student>
    {
        public StudentMap()
        {
            // Primary Key
            this.HasKey(t => t.StudentId);

            // Properties
            // Table & Column Mappings
            this.ToTable("dbo.Student");

            this.Property(t => t.StudentId).HasColumnName("StudentId");
            this.Property(t => t.Height).HasColumnName("Height");
            this.Property(t => t.Weight).HasColumnName("Weight");
            this.Property(t => t.Photo).HasColumnName("Photo");
            this.Property(t => t.StandardId).HasColumnName("StandardId");
            this.Property(t => t.StudentName).HasColumnName("StudentName");

            // Relationships
            this.HasRequired(t => t.Standard)
                .WithMany(t => t.Students)
                .HasForeignKey(d => d.StandardId);

        }
    }
}
