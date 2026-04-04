using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using EfCoreProxyTests.Models;

namespace EfCoreProxyTests;

public class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }

    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Tag> Tags => Set<Tag>();
    public DbSet<AuditLog> AuditLogs => Set<AuditLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Blog>(b =>
        {
            b.HasKey(e => e.Id);
            b.Property(e => e.RowVersion).IsRowVersion();
            b.HasIndex(e => e.Title);
        });

        modelBuilder.Entity<Post>(p =>
        {
            p.HasKey(e => e.Id);
            p.HasOne(e => e.Blog)
                .WithMany(e => e.Posts)
                .HasForeignKey(e => e.BlogId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<Tag>(t =>
        {
            t.HasKey(e => e.Id);
            t.HasMany(e => e.Posts)
                .WithMany(e => e.Tags)
                .UsingEntity("PostTag");
        });

        modelBuilder.Entity<AuditLog>(a =>
        {
            a.HasKey(e => e.Id);
            a.Property(e => e.Id).UseIdentityColumn();
            a.Property(e => e.Timestamp).HasDefaultValueSql("SYSUTCDATETIME()");
        });
    }

    public override int SaveChanges()
    {
        AddAuditEntries();
        return base.SaveChanges();
    }

    public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        AddAuditEntries();
        return await base.SaveChangesAsync(cancellationToken);
    }

    private void AddAuditEntries()
    {
        var entries = ChangeTracker.Entries()
            .Where(e => e.Entity is not AuditLog &&
                        e.State is EntityState.Added or EntityState.Modified or EntityState.Deleted)
            .ToList();

        foreach (var entry in entries)
        {
            var idProp = entry.Properties.FirstOrDefault(p => p.Metadata.Name == "Id");
            if (idProp is null) continue;

            var action = entry.State switch
            {
                EntityState.Added => "INSERT",
                EntityState.Modified => "UPDATE",
                EntityState.Deleted => "DELETE",
                _ => "UNKNOWN"
            };

            var entityId = idProp.CurrentValue as int? ?? 0;

            AuditLogs.Add(new AuditLog
            {
                Action = action,
                EntityName = entry.Entity.GetType().Name,
                EntityId = entityId
            });
        }
    }

    public static TestDbContext Create(string connectionString, bool enableRetry = true)
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
        optionsBuilder.UseSqlServer(connectionString, sqlOptions =>
        {
            if (enableRetry)
                sqlOptions.EnableRetryOnFailure(maxRetryCount: 3);
        });
        return new TestDbContext(optionsBuilder.Options);
    }

    public static TestDbContext CreateWithInterceptor(
        string connectionString, IInterceptor interceptor, bool enableRetry = true)
    {
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
        optionsBuilder.UseSqlServer(connectionString, sqlOptions =>
        {
            if (enableRetry)
                sqlOptions.EnableRetryOnFailure(maxRetryCount: 3);
        });
        optionsBuilder.AddInterceptors(interceptor);
        return new TestDbContext(optionsBuilder.Options);
    }
}
