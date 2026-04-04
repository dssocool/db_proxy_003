using System.ComponentModel.DataAnnotations;

namespace EfCoreProxyTests.Models;

public class Post
{
    public int Id { get; set; }

    [MaxLength(200)]
    public string Title { get; set; } = string.Empty;

    [MaxLength(4000)]
    public string Content { get; set; } = string.Empty;

    public DateTime? PublishedOn { get; set; }

    public int BlogId { get; set; }
    public Blog Blog { get; set; } = null!;

    public List<Tag> Tags { get; set; } = new();
}
