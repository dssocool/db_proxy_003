using System.ComponentModel.DataAnnotations;

namespace EfCoreProxyTests.Models;

public class Blog
{
    public int Id { get; set; }

    [MaxLength(200)]
    public string Title { get; set; } = string.Empty;

    [MaxLength(500)]
    public string Url { get; set; } = string.Empty;

    public int Rating { get; set; }

    [Timestamp]
    public byte[] RowVersion { get; set; } = null!;

    public List<Post> Posts { get; set; } = new();
}
