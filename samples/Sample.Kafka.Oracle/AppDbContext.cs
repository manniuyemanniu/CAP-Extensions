using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace Sample.Kafka.Oracle
{
    public class Person
    {
        [Key]
        public int Id { get; set; }

        public string Name { get; set; }

        public override string ToString()
        {
            return $"Name:{Name}, Id:{Id}";
        }
    }


    public class AppDbContext : DbContext
    {
        public const string ConnectionString = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.20)(PORT=1525))(CONNECT_DATA=(SERVICE_NAME=ORCLCDB)));User Id=CAP;Password=cap123";
        public DbSet<Person> Persons { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseOracle(ConnectionString, c =>
            {
                //Oracle version
                c.UseOracleSQLCompatibility("11");
            });
        }
    }
}
