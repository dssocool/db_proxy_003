using System.ComponentModel.DataAnnotations;

namespace EfCoreProxyTests.Models;

public class Tag
{
    public int Id { get; set; }

    [MaxLength(100)]
    public string Name { get; set; } = string.Empty;

    public List<Post> Posts { get; set; } = new();
}
