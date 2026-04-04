using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace EfCoreProxyTests;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<TestDbContext>
{
    public TestDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
        optionsBuilder.UseSqlServer(
            "Server=127.0.0.1,1433;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true;Encrypt=true;Database=MitmEfCoreTestDb;");
        return new TestDbContext(optionsBuilder.Options);
    }
}
