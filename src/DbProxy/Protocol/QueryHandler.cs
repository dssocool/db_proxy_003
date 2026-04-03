using System.Buffers.Binary;
using System.Text;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

public sealed class QueryHandler
{
    private readonly ILogger _logger;

    public QueryHandler(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Parses a SQL_BATCH payload: strips the ALL_HEADERS prefix and extracts the UTF-16LE SQL text.
    /// ALL_HEADERS starts with a DWORD TotalLength, followed by individual headers.
    /// The SQL text follows immediately after.
    /// </summary>
    public string ParseSqlBatch(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4)
            throw new InvalidOperationException("SQL_BATCH payload too short");

        uint totalHeadersLength = BinaryPrimitives.ReadUInt32LittleEndian(payload);

        // TotalLength includes itself (4 bytes). If it's unreasonably large, treat payload as raw SQL.
        if (totalHeadersLength > (uint)payload.Length || totalHeadersLength < 4)
        {
            _logger.LogWarning("ALL_HEADERS length {Len} seems invalid, treating entire payload as SQL", totalHeadersLength);
            return Encoding.Unicode.GetString(payload);
        }

        var sqlBytes = payload[(int)totalHeadersLength..];
        string sql = Encoding.Unicode.GetString(sqlBytes);

        _logger.LogDebug("Parsed SQL_BATCH ({SqlLen} chars): {Sql}",
            sql.Length, sql.Length > 200 ? sql[..200] + "..." : sql);

        return sql;
    }
}
