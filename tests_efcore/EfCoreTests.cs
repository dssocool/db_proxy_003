using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using EfCoreProxyTests.Models;

namespace EfCoreProxyTests;

public class CommandCapturingInterceptor : DbCommandInterceptor
{
    public List<string> Commands { get; } = new();

    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        Commands.Add(command.CommandText);
        return base.ReaderExecuting(command, eventData, result);
    }

    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        Commands.Add(command.CommandText);
        return base.NonQueryExecuting(command, eventData, result);
    }

    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        Commands.Add(command.CommandText);
        return base.ScalarExecuting(command, eventData, result);
    }
}

public static class EfCoreTests
{
    public static void RunAllTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunLinqQueryTests(connStr, results);
        RunNavigationTests(connStr, results);
        RunSaveChangesTests(connStr, results);
        RunConcurrencyTests(connStr, results);
        RunRetryStrategyTests(connStr, results);
        RunMigrationMetadataTests(connStr, results);
        RunRawSqlTests(connStr, results);
        RunTransactionTests(connStr, results);
        RunCompiledQueryTests(connStr, results);
        RunTrackingTests(connStr, results);
        RunInterceptorTests(connStr, results);
        RunAsyncStreamingTests(connStr, results);
    }

    private static void Run(string name, string connStr,
        List<(string Name, bool Ok, string Data, string? Error)> results,
        Func<TestDbContext, string> action, bool enableRetry = true)
    {
        Console.Write($"  {name} ... ");
        try
        {
            using var context = TestDbContext.Create(connStr, enableRetry);
            var data = action(context);
            results.Add((name, true, data, null));
            Console.WriteLine("PASS");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", $"{ex.GetType().Name}: {ex.Message}"));
            Console.WriteLine("FAIL");
        }
    }

    private static void RunAsync(string name, string connStr,
        List<(string Name, bool Ok, string Data, string? Error)> results,
        Func<TestDbContext, Task<string>> action, bool enableRetry = true)
    {
        Console.Write($"  {name} ... ");
        try
        {
            using var context = TestDbContext.Create(connStr, enableRetry);
            var data = action(context).GetAwaiter().GetResult();
            results.Add((name, true, data, null));
            Console.WriteLine("PASS");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", $"{ex.GetType().Name}: {ex.Message}"));
            Console.WriteLine("FAIL");
        }
    }

    private static void RunIntercepted(string name, string connStr,
        List<(string Name, bool Ok, string Data, string? Error)> results,
        Func<TestDbContext, CommandCapturingInterceptor, string> action, bool enableRetry = true)
    {
        Console.Write($"  {name} ... ");
        try
        {
            var interceptor = new CommandCapturingInterceptor();
            using var context = TestDbContext.CreateWithInterceptor(connStr, interceptor, enableRetry);
            var data = action(context, interceptor);
            results.Add((name, true, data, null));
            Console.WriteLine("PASS");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", $"{ex.GetType().Name}: {ex.Message}"));
            Console.WriteLine("FAIL");
        }
    }

    private static string FormatBlogs(IEnumerable<Blog> blogs)
    {
        var sb = new StringBuilder();
        foreach (var b in blogs)
            sb.AppendLine($"Blog|Id={b.Id}|Title={b.Title}|Url={b.Url}|Rating={b.Rating}");
        return sb.ToString().TrimEnd();
    }

    private static string FormatPosts(IEnumerable<Post> posts)
    {
        var sb = new StringBuilder();
        foreach (var p in posts)
            sb.AppendLine($"Post|Id={p.Id}|Title={p.Title}|BlogId={p.BlogId}|Published={p.PublishedOn?.ToString("yyyy-MM-dd") ?? "NULL"}");
        return sb.ToString().TrimEnd();
    }

    // ==================== 1. LINQ Query Translation ====================

    private static void RunLinqQueryTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- LINQ Query Translation ---");

        Run("EF_SELECT_ALL", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs.OrderBy(b => b.Id).ToList();
            return FormatBlogs(blogs);
        });

        Run("EF_WHERE_FILTER", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs.Where(b => b.Rating > 3).OrderBy(b => b.Id).ToList();
            return FormatBlogs(blogs);
        });

        Run("EF_ORDERBY_PAGING", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs.OrderBy(b => b.Title).Skip(1).Take(2).ToList();
            return FormatBlogs(blogs);
        });

        Run("EF_PROJECTION", connStr, results, ctx =>
        {
            var items = ctx.Blogs.OrderBy(b => b.Id)
                .Select(b => new { b.Title, b.Rating })
                .ToList();
            var sb = new StringBuilder();
            foreach (var i in items)
                sb.AppendLine($"Proj|Title={i.Title}|Rating={i.Rating}");
            return sb.ToString().TrimEnd();
        });

        Run("EF_FIRST_SINGLE", connStr, results, ctx =>
        {
            var first = ctx.Blogs.OrderBy(b => b.Id).FirstOrDefault();
            var single = ctx.Blogs.Where(b => b.Rating == 1).SingleOrDefault();
            var sb = new StringBuilder();
            sb.AppendLine($"First|Title={first?.Title}|Rating={first?.Rating}");
            sb.AppendLine($"Single|Title={single?.Title}|Rating={single?.Rating}");
            return sb.ToString().TrimEnd();
        });

        Run("EF_COUNT_ANY", connStr, results, ctx =>
        {
            var count = ctx.Blogs.Count();
            var any = ctx.Blogs.Any(b => b.Rating > 4);
            var noMatch = ctx.Blogs.Any(b => b.Rating > 100);
            return $"Count={count}|AnyAbove4={any}|AnyAbove100={noMatch}";
        });

        Run("EF_GROUPBY", connStr, results, ctx =>
        {
            var groups = ctx.Posts
                .GroupBy(p => p.BlogId)
                .Select(g => new { BlogId = g.Key, PostCount = g.Count() })
                .OrderBy(g => g.BlogId)
                .ToList();
            var sb = new StringBuilder();
            foreach (var g in groups)
                sb.AppendLine($"Group|BlogId={g.BlogId}|PostCount={g.PostCount}");
            return sb.ToString().TrimEnd();
        });
    }

    // ==================== 2. Navigation / Include / Split Queries ====================

    private static void RunNavigationTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Navigation / Include / Split Queries ---");

        Run("EF_EAGER_INCLUDE", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs.Include(b => b.Posts).OrderBy(b => b.Id).ToList();
            var sb = new StringBuilder();
            foreach (var b in blogs)
            {
                sb.AppendLine($"Blog|Title={b.Title}|PostCount={b.Posts.Count}");
                foreach (var p in b.Posts.OrderBy(p => p.Id))
                    sb.AppendLine($"  Post|Title={p.Title}");
            }
            return sb.ToString().TrimEnd();
        });

        Run("EF_THEN_INCLUDE", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs
                .Include(b => b.Posts)
                    .ThenInclude(p => p.Tags)
                .OrderBy(b => b.Id)
                .ToList();
            var sb = new StringBuilder();
            foreach (var b in blogs)
            {
                sb.AppendLine($"Blog|Title={b.Title}");
                foreach (var p in b.Posts.OrderBy(p => p.Id))
                {
                    var tagNames = string.Join(",", p.Tags.OrderBy(t => t.Name).Select(t => t.Name));
                    sb.AppendLine($"  Post|Title={p.Title}|Tags={tagNames}");
                }
            }
            return sb.ToString().TrimEnd();
        });

        Run("EF_SPLIT_QUERY", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs
                .Include(b => b.Posts)
                .AsSplitQuery()
                .OrderBy(b => b.Id)
                .ToList();
            var sb = new StringBuilder();
            foreach (var b in blogs)
            {
                sb.AppendLine($"Blog|Title={b.Title}|PostCount={b.Posts.Count}");
            }
            return sb.ToString().TrimEnd();
        });

        Run("EF_EXPLICIT_LOAD", connStr, results, ctx =>
        {
            var blog = ctx.Blogs.OrderBy(b => b.Id).First();
            ctx.Entry(blog).Collection(b => b.Posts).Load();
            var sb = new StringBuilder();
            sb.AppendLine($"Blog|Title={blog.Title}|PostCount={blog.Posts.Count}");
            foreach (var p in blog.Posts.OrderBy(p => p.Id))
                sb.AppendLine($"  Post|Title={p.Title}");
            return sb.ToString().TrimEnd();
        });

        Run("EF_MANY_TO_MANY", connStr, results, ctx =>
        {
            var posts = ctx.Posts
                .Include(p => p.Tags)
                .Where(p => p.Tags.Any(t => t.Name == "efcore"))
                .OrderBy(p => p.Id)
                .ToList();
            var sb = new StringBuilder();
            foreach (var p in posts)
            {
                var tagNames = string.Join(",", p.Tags.OrderBy(t => t.Name).Select(t => t.Name));
                sb.AppendLine($"Post|Title={p.Title}|Tags={tagNames}");
            }
            return sb.ToString().TrimEnd();
        });
    }

    // ==================== 3. SaveChanges Batching and Ordering ====================

    private static void RunSaveChangesTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- SaveChanges Batching and Ordering ---");

        Run("EF_INSERT_SINGLE", connStr, results, ctx =>
        {
            var blog = new Blog { Title = "Insert Test", Url = "https://insert.test", Rating = 7 };
            ctx.Blogs.Add(blog);
            var saved = ctx.SaveChanges();
            var result = $"SavedCount={saved}|GeneratedId={blog.Id}|HasId={blog.Id > 0}";
            ctx.Blogs.Remove(blog);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_INSERT_BATCH", connStr, results, ctx =>
        {
            var blogs = Enumerable.Range(0, 5).Select(i =>
                new Blog { Title = $"Batch {i}", Url = $"https://batch{i}.test", Rating = i }).ToList();
            ctx.Blogs.AddRange(blogs);
            var saved = ctx.SaveChanges();
            var allHaveIds = blogs.All(b => b.Id > 0);
            var distinctIds = blogs.Select(b => b.Id).Distinct().Count();
            var result = $"SavedCount={saved}|AllHaveIds={allHaveIds}|DistinctIds={distinctIds}";
            ctx.Blogs.RemoveRange(blogs);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_INSERT_GRAPH", connStr, results, ctx =>
        {
            var blog = new Blog
            {
                Title = "Graph Insert",
                Url = "https://graph.test",
                Rating = 6,
                Posts = new()
                {
                    new() { Title = "Graph Post 1", Content = "Content 1" },
                    new() { Title = "Graph Post 2", Content = "Content 2" }
                }
            };
            ctx.Blogs.Add(blog);
            var saved = ctx.SaveChanges();
            var postsFk = blog.Posts.All(p => p.BlogId == blog.Id);
            var result = $"SavedCount={saved}|BlogId={blog.Id}|PostsFkCorrect={postsFk}|PostCount={blog.Posts.Count}";
            ctx.Posts.RemoveRange(blog.Posts);
            ctx.Blogs.Remove(blog);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_UPDATE_SINGLE", connStr, results, ctx =>
        {
            var blog = new Blog { Title = "Update Test", Url = "https://update.test", Rating = 1 };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();

            blog.Title = "Updated Title";
            blog.Rating = 10;
            var saved = ctx.SaveChanges();

            var reloaded = ctx.Blogs.AsNoTracking().Single(b => b.Id == blog.Id);
            var result = $"SavedCount={saved}|Title={reloaded.Title}|Rating={reloaded.Rating}";
            ctx.Blogs.Remove(blog);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_UPDATE_BATCH", connStr, results, ctx =>
        {
            var blogs = Enumerable.Range(0, 3).Select(i =>
                new Blog { Title = $"BatchUpd {i}", Url = $"https://batchupd{i}.test", Rating = i }).ToList();
            ctx.Blogs.AddRange(blogs);
            ctx.SaveChanges();

            foreach (var b in blogs) b.Rating += 100;
            var saved = ctx.SaveChanges();

            var reloaded = ctx.Blogs.AsNoTracking()
                .Where(b => blogs.Select(x => x.Id).Contains(b.Id))
                .OrderBy(b => b.Id)
                .ToList();
            var allUpdated = reloaded.All(b => b.Rating >= 100);
            var result = $"SavedCount={saved}|AllUpdated={allUpdated}";
            ctx.Blogs.RemoveRange(blogs);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_DELETE_SINGLE", connStr, results, ctx =>
        {
            var blog = new Blog { Title = "Delete Me", Url = "https://delete.test", Rating = 0 };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();
            var id = blog.Id;

            ctx.Blogs.Remove(blog);
            var saved = ctx.SaveChanges();
            var exists = ctx.Blogs.AsNoTracking().Any(b => b.Id == id);
            return $"SavedCount={saved}|StillExists={exists}";
        });

        Run("EF_DELETE_CASCADE", connStr, results, ctx =>
        {
            var blog = new Blog
            {
                Title = "Cascade Delete",
                Url = "https://cascade.test",
                Rating = 0,
                Posts = new()
                {
                    new() { Title = "Cascade Post 1", Content = "Will be deleted" },
                    new() { Title = "Cascade Post 2", Content = "Also deleted" }
                }
            };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();
            var blogId = blog.Id;
            var postIds = blog.Posts.Select(p => p.Id).ToList();

            ctx.Blogs.Remove(blog);
            var saved = ctx.SaveChanges();
            var blogExists = ctx.Blogs.AsNoTracking().Any(b => b.Id == blogId);
            var postsExist = ctx.Posts.AsNoTracking().Any(p => postIds.Contains(p.Id));
            return $"SavedCount={saved}|BlogExists={blogExists}|PostsExist={postsExist}";
        });

        Run("EF_MIXED_OPERATIONS", connStr, results, ctx =>
        {
            var insertBlog = new Blog { Title = "Mixed Insert", Url = "https://mixed-i.test", Rating = 1 };
            ctx.Blogs.Add(insertBlog);
            ctx.SaveChanges();

            var updateBlog = ctx.Blogs.First(b => b.Title == "Mixed Insert");
            updateBlog.Title = "Mixed Updated";

            var deleteBlog = new Blog { Title = "Mixed Delete", Url = "https://mixed-d.test", Rating = 0 };
            ctx.Blogs.Add(deleteBlog);
            ctx.SaveChanges();

            ctx.Blogs.Remove(deleteBlog);
            updateBlog.Rating = 99;

            var newBlog = new Blog { Title = "Mixed New", Url = "https://mixed-n.test", Rating = 5 };
            ctx.Blogs.Add(newBlog);

            var saved = ctx.SaveChanges();

            var updatedExists = ctx.Blogs.AsNoTracking().Any(b => b.Title == "Mixed Updated" && b.Rating == 99);
            var deletedGone = !ctx.Blogs.AsNoTracking().Any(b => b.Title == "Mixed Delete");
            var newExists = ctx.Blogs.AsNoTracking().Any(b => b.Title == "Mixed New");

            var result = $"SavedCount={saved}|UpdatedExists={updatedExists}|DeletedGone={deletedGone}|NewExists={newExists}";

            ctx.Blogs.RemoveRange(ctx.Blogs.Where(b =>
                b.Title == "Mixed Updated" || b.Title == "Mixed New"));
            ctx.SaveChanges();
            return result;
        });
    }

    // ==================== 4. Optimistic Concurrency ====================

    private static void RunConcurrencyTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Optimistic Concurrency ---");

        Run("EF_CONCURRENCY_SUCCESS", connStr, results, ctx =>
        {
            var blog = new Blog { Title = "Concurrency Test", Url = "https://concurrency.test", Rating = 1 };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();

            blog.Rating = 42;
            var saved = ctx.SaveChanges();

            var reloaded = ctx.Blogs.AsNoTracking().Single(b => b.Id == blog.Id);
            var result = $"SavedCount={saved}|Rating={reloaded.Rating}|HasRowVersion={reloaded.RowVersion?.Length > 0}";
            ctx.Blogs.Remove(blog);
            ctx.SaveChanges();
            return result;
        });

        Run("EF_CONCURRENCY_CONFLICT", connStr, results, ctx =>
        {
            var blog = new Blog { Title = "Conflict Test", Url = "https://conflict.test", Rating = 1 };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();
            var blogId = blog.Id;

            using var ctx2 = TestDbContext.Create(connStr);
            var blog2 = ctx2.Blogs.Single(b => b.Id == blogId);
            blog2.Rating = 999;
            ctx2.SaveChanges();

            blog.Rating = 888;
            bool conflictCaught = false;
            string exceptionType = "";
            try
            {
                ctx.SaveChanges();
            }
            catch (DbUpdateConcurrencyException ex)
            {
                conflictCaught = true;
                exceptionType = ex.GetType().Name;
            }

            using var ctx3 = TestDbContext.Create(connStr);
            var final = ctx3.Blogs.AsNoTracking().Single(b => b.Id == blogId);
            var result = $"ConflictCaught={conflictCaught}|ExceptionType={exceptionType}|FinalRating={final.Rating}";
            ctx3.Blogs.Remove(ctx3.Blogs.Single(b => b.Id == blogId));
            ctx3.SaveChanges();
            return result;
        });
    }

    // ==================== 5. Execution Strategy / Retry ====================

    private static void RunRetryStrategyTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Execution Strategy / Retry ---");

        Run("EF_RETRY_STRATEGY", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs.OrderBy(b => b.Id).ToList();
            return $"RetryEnabled=True|BlogCount={blogs.Count}";
        });

        Run("EF_RETRY_MANUAL_TRANSACTION", connStr, results, ctx =>
        {
            var strategy = ctx.Database.CreateExecutionStrategy();
            var result = "";

            strategy.Execute(() =>
            {
                using var transaction = ctx.Database.BeginTransaction();
                var blog = new Blog { Title = "Retry Txn Test", Url = "https://retry-txn.test", Rating = 3 };
                ctx.Blogs.Add(blog);
                ctx.SaveChanges();

                blog.Rating = 33;
                ctx.SaveChanges();

                transaction.Commit();

                var reloaded = ctx.Blogs.AsNoTracking().Single(b => b.Id == blog.Id);
                result = $"Rating={reloaded.Rating}|Title={reloaded.Title}";

                ctx.Blogs.Remove(blog);
                ctx.SaveChanges();
            });

            return result;
        });
    }

    // ==================== 6. Migrations and Model Metadata ====================

    private static void RunMigrationMetadataTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Migrations and Model Metadata ---");

        Run("EF_MIGRATION_APPLY", connStr, results, ctx =>
        {
            var pending = ctx.Database.GetPendingMigrations().ToList();
            var applied = ctx.Database.GetAppliedMigrations().ToList();
            return $"PendingCount={pending.Count}|AppliedCount={applied.Count}|CanConnect={ctx.Database.CanConnect()}";
        });

        Run("EF_MIGRATION_SCRIPT", connStr, results, ctx =>
        {
            var script = ctx.Database.GenerateCreateScript();
            var hasCreateTable = script.Contains("CREATE TABLE");
            var tableCount = script.Split("CREATE TABLE").Length - 1;
            return $"HasCreateTable={hasCreateTable}|TableCount={tableCount}|ScriptLength={script.Length}";
        });

        Run("EF_MODEL_METADATA", connStr, results, ctx =>
        {
            var entityTypes = ctx.Model.GetEntityTypes()
                .OrderBy(e => e.Name)
                .ToList();
            var sb = new StringBuilder();
            foreach (var et in entityTypes)
            {
                var props = et.GetProperties()
                    .OrderBy(p => p.Name)
                    .Select(p => $"{p.Name}:{p.ClrType.Name}")
                    .ToList();
                sb.AppendLine($"Entity|{et.ShortName()}|Props={string.Join(",", props)}");
            }
            return sb.ToString().TrimEnd();
        });
    }

    // ==================== 7. Raw SQL and Interpolated Queries ====================

    private static void RunRawSqlTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Raw SQL and Interpolated Queries ---");

        Run("EF_RAW_SQL", connStr, results, ctx =>
        {
            var blogs = ctx.Blogs
                .FromSqlRaw("SELECT * FROM Blogs WHERE Rating > {0}", 3)
                .OrderBy(b => b.Id)
                .ToList();
            return FormatBlogs(blogs);
        });

        Run("EF_INTERPOLATED_SQL", connStr, results, ctx =>
        {
            int minRating = 3;
            var blogs = ctx.Blogs
                .FromSqlInterpolated($"SELECT * FROM Blogs WHERE Rating > {minRating}")
                .OrderBy(b => b.Id)
                .ToList();
            return FormatBlogs(blogs);
        });

        Run("EF_SQL_QUERY_SCALAR", connStr, results, ctx =>
        {
            var counts = ctx.Database
                .SqlQueryRaw<int>("SELECT COUNT(*) AS [Value] FROM Blogs")
                .ToList();
            return $"BlogCount={counts.FirstOrDefault()}";
        });
    }

    // ==================== 8. Transaction Scopes ====================

    private static void RunTransactionTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Transaction Scopes ---");

        Run("EF_EXPLICIT_TRANSACTION", connStr, results, ctx =>
        {
            var strategy = ctx.Database.CreateExecutionStrategy();
            var result = "";

            strategy.Execute(() =>
            {
                using var txn = ctx.Database.BeginTransaction();
                var blog = new Blog { Title = "Txn Commit Test", Url = "https://txn.test", Rating = 7 };
                ctx.Blogs.Add(blog);
                ctx.SaveChanges();
                txn.Commit();

                var exists = ctx.Blogs.AsNoTracking().Any(b => b.Title == "Txn Commit Test");
                result = $"Committed={exists}";

                ctx.Blogs.Remove(blog);
                ctx.SaveChanges();
            });

            return result;
        }, enableRetry: true);

        Run("EF_TRANSACTION_ROLLBACK", connStr, results, ctx =>
        {
            var strategy = ctx.Database.CreateExecutionStrategy();
            var result = "";

            strategy.Execute(() =>
            {
                using (var txn = ctx.Database.BeginTransaction())
                {
                    var blog = new Blog { Title = "Txn Rollback Test", Url = "https://rollback.test", Rating = 0 };
                    ctx.Blogs.Add(blog);
                    ctx.SaveChanges();
                    txn.Rollback();
                }

                ctx.ChangeTracker.Clear();
                var exists = ctx.Blogs.AsNoTracking().Any(b => b.Title == "Txn Rollback Test");
                result = $"RolledBack={!exists}|StillExists={exists}";
            });

            return result;
        }, enableRetry: true);

        Run("EF_SAVEPOINT", connStr, results, ctx =>
        {
            var strategy = ctx.Database.CreateExecutionStrategy();
            var result = "";

            strategy.Execute(() =>
            {
                using var txn = ctx.Database.BeginTransaction();

                var blog1 = new Blog { Title = "Savepoint Keep", Url = "https://sp1.test", Rating = 1 };
                ctx.Blogs.Add(blog1);
                ctx.SaveChanges();

                txn.CreateSavepoint("BeforeSecondInsert");

                var blog2 = new Blog { Title = "Savepoint Discard", Url = "https://sp2.test", Rating = 2 };
                ctx.Blogs.Add(blog2);
                ctx.SaveChanges();

                txn.RollbackToSavepoint("BeforeSecondInsert");

                txn.Commit();

                ctx.ChangeTracker.Clear();
                var keepExists = ctx.Blogs.AsNoTracking().Any(b => b.Title == "Savepoint Keep");
                var discardGone = !ctx.Blogs.AsNoTracking().Any(b => b.Title == "Savepoint Discard");
                result = $"KeepExists={keepExists}|DiscardGone={discardGone}";

                var cleanup = ctx.Blogs.Where(b => b.Title == "Savepoint Keep");
                ctx.Blogs.RemoveRange(cleanup);
                ctx.SaveChanges();
            });

            return result;
        }, enableRetry: true);
    }

    // ==================== 9. Compiled Queries + ExecuteUpdate/ExecuteDelete ====================

    private static readonly Func<TestDbContext, int, IEnumerable<Blog>> _compiledBlogsByMinRating =
        EF.CompileQuery((TestDbContext ctx, int minRating) =>
            ctx.Blogs.Where(b => b.Rating >= minRating).OrderBy(b => b.Id).AsQueryable());

    private static readonly Func<TestDbContext, int, IAsyncEnumerable<Blog>> _compiledBlogsByMinRatingAsync =
        EF.CompileAsyncQuery((TestDbContext ctx, int minRating) =>
            ctx.Blogs.Where(b => b.Rating >= minRating).OrderBy(b => b.Id).AsQueryable());

    private static void RunCompiledQueryTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Compiled Queries + ExecuteUpdate/ExecuteDelete ---");

        Run("EF_COMPILED_QUERY", connStr, results, ctx =>
        {
            var blogs = _compiledBlogsByMinRating(ctx, 4).ToList();
            return $"Count={blogs.Count}|{FormatBlogs(blogs)}";
        });

        RunAsync("EF_COMPILED_ASYNC_QUERY", connStr, results, async ctx =>
        {
            var blogs = new List<Blog>();
            await foreach (var blog in _compiledBlogsByMinRatingAsync(ctx, 4))
                blogs.Add(blog);
            return $"Count={blogs.Count}|{FormatBlogs(blogs)}";
        });

        Run("EF_EXECUTE_UPDATE", connStr, results, ctx =>
        {
            var testBlogs = Enumerable.Range(0, 3).Select(i =>
                new Blog { Title = $"ExUpd {i}", Url = $"https://exupd{i}.test", Rating = 50 + i }).ToList();
            ctx.Blogs.AddRange(testBlogs);
            ctx.SaveChanges();
            ctx.ChangeTracker.Clear();

            var affected = ctx.Blogs
                .Where(b => b.Title.StartsWith("ExUpd"))
                .ExecuteUpdate(s => s.SetProperty(b => b.Rating, b => b.Rating + 100));

            var updated = ctx.Blogs.AsNoTracking()
                .Where(b => b.Title.StartsWith("ExUpd"))
                .OrderBy(b => b.Id)
                .ToList();
            var allBumped = updated.All(b => b.Rating >= 150);

            ctx.Blogs.Where(b => b.Title.StartsWith("ExUpd")).ExecuteDelete();
            return $"AffectedRows={affected}|AllBumped={allBumped}|Ratings={string.Join(",", updated.Select(b => b.Rating))}";
        });

        Run("EF_EXECUTE_DELETE", connStr, results, ctx =>
        {
            var testBlogs = Enumerable.Range(0, 4).Select(i =>
                new Blog { Title = $"ExDel {i}", Url = $"https://exdel{i}.test", Rating = 0 }).ToList();
            ctx.Blogs.AddRange(testBlogs);
            ctx.SaveChanges();
            ctx.ChangeTracker.Clear();

            var countBefore = ctx.Blogs.AsNoTracking().Count(b => b.Title.StartsWith("ExDel"));
            var affected = ctx.Blogs.Where(b => b.Title.StartsWith("ExDel")).ExecuteDelete();
            var countAfter = ctx.Blogs.AsNoTracking().Count(b => b.Title.StartsWith("ExDel"));

            return $"CountBefore={countBefore}|AffectedRows={affected}|CountAfter={countAfter}";
        });
    }

    // ==================== 10. AsNoTrackingWithIdentityResolution ====================

    private static void RunTrackingTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- AsNoTrackingWithIdentityResolution ---");

        Run("EF_NO_TRACKING_IDENTITY_RESOLUTION", connStr, results, ctx =>
        {
            var posts = ctx.Posts
                .Include(p => p.Blog)
                .AsNoTrackingWithIdentityResolution()
                .Where(p => p.Blog.Title == "EF Core Blog")
                .OrderBy(p => p.Id)
                .ToList();

            if (posts.Count < 2)
                return $"PostCount={posts.Count}|NeedAtLeast2=FAIL";

            var sameInstance = ReferenceEquals(posts[0].Blog, posts[1].Blog);
            return $"PostCount={posts.Count}|SameBlogInstance={sameInstance}|BlogTitle={posts[0].Blog.Title}";
        });

        Run("EF_NO_TRACKING_BASIC", connStr, results, ctx =>
        {
            var posts = ctx.Posts
                .Include(p => p.Blog)
                .AsNoTracking()
                .Where(p => p.Blog.Title == "EF Core Blog")
                .OrderBy(p => p.Id)
                .ToList();

            if (posts.Count < 2)
                return $"PostCount={posts.Count}|NeedAtLeast2=FAIL";

            var sameInstance = ReferenceEquals(posts[0].Blog, posts[1].Blog);
            return $"PostCount={posts.Count}|SameBlogInstance={sameInstance}|BlogTitle={posts[0].Blog.Title}";
        });
    }

    // ==================== 11. DbCommandInterceptor ====================

    private static void RunInterceptorTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- DbCommandInterceptor ---");

        RunIntercepted("EF_INTERCEPTOR_INSERT_ORDER", connStr, results, (ctx, interceptor) =>
        {
            var blog = new Blog
            {
                Title = "Intercepted Graph",
                Url = "https://intercepted.test",
                Rating = 7,
                Posts = new()
                {
                    new() { Title = "Intercepted Post 1", Content = "Content 1" },
                    new() { Title = "Intercepted Post 2", Content = "Content 2" }
                }
            };
            ctx.Blogs.Add(blog);
            ctx.SaveChanges();

            var insertCommands = interceptor.Commands
                .Where(c => c.Contains("INSERT", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var blogInsertIdx = insertCommands.FindIndex(c =>
                c.Contains("[Blogs]", StringComparison.OrdinalIgnoreCase));
            var postInsertIdx = insertCommands.FindIndex(c =>
                c.Contains("[Posts]", StringComparison.OrdinalIgnoreCase));

            var blogBeforePosts = blogInsertIdx >= 0 && postInsertIdx >= 0 && blogInsertIdx < postInsertIdx;

            var result = $"TotalCommands={interceptor.Commands.Count}|InsertCommands={insertCommands.Count}|BlogBeforePosts={blogBeforePosts}";

            ctx.Posts.RemoveRange(blog.Posts);
            ctx.Blogs.Remove(blog);
            ctx.SaveChanges();
            return result;
        });

        RunIntercepted("EF_INTERCEPTOR_COMMAND_SHAPE", connStr, results, (ctx, interceptor) =>
        {
            var _ = ctx.Blogs
                .Where(b => b.Rating > 3)
                .OrderBy(b => b.Title)
                .ToList();

            var queryCmd = interceptor.Commands
                .FirstOrDefault(c => c.Contains("SELECT", StringComparison.OrdinalIgnoreCase)
                                  && c.Contains("[Blogs]", StringComparison.OrdinalIgnoreCase));

            if (queryCmd is null)
                return "QueryCaptured=False";

            var hasSelect = queryCmd.Contains("SELECT", StringComparison.OrdinalIgnoreCase);
            var hasFrom = queryCmd.Contains("FROM", StringComparison.OrdinalIgnoreCase);
            var hasWhere = queryCmd.Contains("WHERE", StringComparison.OrdinalIgnoreCase);
            var hasOrderBy = queryCmd.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase);

            return $"QueryCaptured=True|HasSELECT={hasSelect}|HasFROM={hasFrom}|HasWHERE={hasWhere}|HasORDERBY={hasOrderBy}";
        });
    }

    // ==================== 12. Async Streaming + Cancellation ====================

    private static void RunAsyncStreamingTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        Console.WriteLine("--- Async Streaming + Cancellation ---");

        RunAsync("EF_ASYNC_STREAMING", connStr, results, async ctx =>
        {
            var items = new List<Blog>();
            await foreach (var blog in ctx.Blogs.OrderBy(b => b.Id).AsAsyncEnumerable())
                items.Add(blog);

            var expected = ctx.Blogs.Count();
            return $"StreamedCount={items.Count}|ExpectedCount={expected}|Match={items.Count == expected}|{FormatBlogs(items)}";
        });

        RunAsync("EF_ASYNC_CANCEL_MIDSTREAM", connStr, results, async ctx =>
        {
            var cts = new CancellationTokenSource();
            int consumed = 0;
            bool cancelled = false;
            string exType = "";
            const int cancelAfter = 2;

            try
            {
                await foreach (var blog in ctx.Blogs.OrderBy(b => b.Id).AsAsyncEnumerable().WithCancellation(cts.Token))
                {
                    consumed++;
                    if (consumed >= cancelAfter)
                        cts.Cancel();
                }
            }
            catch (OperationCanceledException ex)
            {
                cancelled = true;
                exType = ex.GetType().Name;
            }

            return $"Consumed={consumed}|CancelAfter={cancelAfter}|Cancelled={cancelled}|ExceptionType={exType}";
        });
    }
}
