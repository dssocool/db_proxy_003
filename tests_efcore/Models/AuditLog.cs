using System.ComponentModel.DataAnnotations;

namespace EfCoreProxyTests.Models;

public class AuditLog
{
    public int Id { get; set; }

    [MaxLength(50)]
    public string Action { get; set; } = string.Empty;

    [MaxLength(100)]
    public string EntityName { get; set; } = string.Empty;

    public int EntityId { get; set; }

    public DateTime Timestamp { get; set; }
}
