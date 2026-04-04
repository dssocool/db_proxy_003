using EfCoreProxyTests;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

const string TestDbName = "MitmEfCoreTestDb";

const string DirectConnBase =
    "Server=127.0.0.1,1433;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true;Encrypt=true;";
const string ProxyConnBase =
    "Server=127.0.0.1,21433;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true;Encrypt=false;";

string DirectConn = DirectConnBase + $"Database={TestDbName};";
string ProxyConn = ProxyConnBase + $"Database={TestDbName};";

string mode = args.Length > 0 ? args[0] : "direct";

if (mode == "setup")
{
    Console.WriteLine($"Setting up EF Core test database [{TestDbName}]...");

    using (var conn = new SqlConnection(DirectConnBase + "Database=master;"))
    {
        conn.Open();
        using var cmd = new SqlCommand($@"
            IF DB_ID('{TestDbName}') IS NULL
                CREATE DATABASE [{TestDbName}];", conn);
        cmd.ExecuteNonQuery();
    }
    Console.WriteLine("  Database created (or already exists).");

    using var context = TestDbContext.Create(DirectConn, enableRetry: false);
    context.Database.Migrate();
    Console.WriteLine("  Migrations applied.");

    SeedData(context);
    Console.WriteLine("Setup complete.");
    return;
}

if (mode == "teardown")
{
    Console.WriteLine($"Tearing down EF Core test database [{TestDbName}]...");
    using var conn = new SqlConnection(DirectConnBase + "Database=master;");
    conn.Open();
    using var cmd = new SqlCommand($@"
        IF DB_ID('{TestDbName}') IS NOT NULL
        BEGIN
            ALTER DATABASE [{TestDbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [{TestDbName}];
        END", conn);
    cmd.ExecuteNonQuery();
    Console.WriteLine("Done.");
    return;
}

if (mode == "compare")
{
    bool strict = args.Length > 1 && args[1] == "--strict";
    var baseDir = AppContext.BaseDirectory;
    EfCoreCompare.CompareResults(
        Path.Combine(baseDir, "results_direct.txt"),
        Path.Combine(baseDir, "results_proxy.txt"),
        strict);
    return;
}

string connStr = mode == "proxy" ? ProxyConn : DirectConn;
string label = mode == "proxy" ? "PROXY" : "DIRECT";

Console.WriteLine($"===== Running EF Core tests against {label} ({connStr.Split(';')[0]}) =====");
Console.WriteLine();

var results = new List<(string Name, bool Ok, string Data, string? Error)>();

EfCoreTests.RunAllTests(connStr, results);

Console.WriteLine();
Console.WriteLine($"===== {label} Summary: {results.Count(r => r.Ok)} passed, {results.Count(r => !r.Ok)} failed out of {results.Count} =====");

var outputFile = Path.Combine(AppContext.BaseDirectory, $"results_{mode}.txt");
using (var writer = new StreamWriter(outputFile))
{
    foreach (var (name, ok, data, error) in results)
    {
        writer.WriteLine($"### {name}");
        writer.WriteLine($"STATUS: {(ok ? "PASS" : "FAIL")}");
        if (ok)
            writer.WriteLine(data);
        else
            writer.WriteLine($"ERROR: {error}");
        writer.WriteLine("###END###");
    }
}
Console.WriteLine($"Results saved to: {outputFile}");

static void SeedData(TestDbContext context)
{
    if (context.Blogs.Any()) return;

    var tags = new[]
    {
        new EfCoreProxyTests.Models.Tag { Name = "csharp" },
        new EfCoreProxyTests.Models.Tag { Name = "efcore" },
        new EfCoreProxyTests.Models.Tag { Name = "dotnet" },
        new EfCoreProxyTests.Models.Tag { Name = "sql" },
        new EfCoreProxyTests.Models.Tag { Name = "testing" }
    };
    context.Tags.AddRange(tags);

    var blog1 = new EfCoreProxyTests.Models.Blog
    {
        Title = "EF Core Blog",
        Url = "https://example.com/efcore",
        Rating = 5,
        Posts = new()
        {
            new() { Title = "Getting Started", Content = "Introduction to EF Core", PublishedOn = new DateTime(2024, 1, 15), Tags = { tags[0], tags[1] } },
            new() { Title = "Advanced Queries", Content = "LINQ deep dive", PublishedOn = new DateTime(2024, 2, 20), Tags = { tags[1], tags[2] } },
            new() { Title = "Performance Tips", Content = "Optimizing EF Core", PublishedOn = null, Tags = { tags[1] } }
        }
    };

    var blog2 = new EfCoreProxyTests.Models.Blog
    {
        Title = "SQL Server Blog",
        Url = "https://example.com/sql",
        Rating = 4,
        Posts = new()
        {
            new() { Title = "Indexes Deep Dive", Content = "All about indexes", PublishedOn = new DateTime(2024, 3, 10), Tags = { tags[3] } },
            new() { Title = "Query Plans", Content = "Understanding execution plans", PublishedOn = new DateTime(2024, 4, 5), Tags = { tags[3], tags[4] } }
        }
    };

    var blog3 = new EfCoreProxyTests.Models.Blog
    {
        Title = "Testing Blog",
        Url = "https://example.com/testing",
        Rating = 3,
        Posts = new()
        {
            new() { Title = "Unit Testing 101", Content = "Basics of unit testing", PublishedOn = new DateTime(2024, 5, 1), Tags = { tags[4], tags[0] } }
        }
    };

    var blog4 = new EfCoreProxyTests.Models.Blog
    {
        Title = "Empty Blog",
        Url = "https://example.com/empty",
        Rating = 1
    };

    context.Blogs.AddRange(blog1, blog2, blog3, blog4);
    context.SaveChanges();

    context.ChangeTracker.Clear();
    context.AuditLogs.RemoveRange(context.AuditLogs);
    context.SaveChanges();

    Console.WriteLine($"  Seeded {context.Blogs.Count()} blogs, {context.Posts.Count()} posts, {context.Tags.Count()} tags.");
}
