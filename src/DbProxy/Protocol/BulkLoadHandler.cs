using System.Buffers.Binary;
using System.Data;
using System.Text;
using DbProxy.Tds;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

/// <summary>
/// Handles TDS Bulk Load BCP (packet type 0x07).
///
/// The flow is:
///   1. Client sends SQL_BATCH "INSERT BULK tablename (col defs) ..." — handled by <see cref="TryParseInsertBulk"/>.
///      The proxy responds with a DONE token (no actual execution on backend yet).
///   2. Client sends BulkLoadBCP (0x07) containing COLMETADATA + ROW* + DONE.
///      The proxy parses rows into a DataTable and uses SqlBulkCopy to write to the backend.
/// </summary>
public sealed class BulkLoadHandler
{
    private readonly ILogger _logger;

    private string? _pendingTableName;
    private List<BulkColumnMapping>? _pendingColumnMappings;

    public BulkLoadHandler(ILogger logger)
    {
        _logger = logger;
    }

    public bool HasPendingBulkInsert => _pendingTableName != null;

    /// <summary>
    /// Tries to parse an INSERT BULK statement and stash the metadata for the upcoming 0x07 message.
    /// Returns true if the SQL was an INSERT BULK (caller should send a DONE and not execute on backend).
    /// </summary>
    public bool TryParseInsertBulk(string sql)
    {
        var trimmed = sql.TrimStart();
        if (!trimmed.StartsWith("INSERT BULK", StringComparison.OrdinalIgnoreCase))
            return false;

        _pendingTableName = ParseTableName(trimmed);
        _pendingColumnMappings = ParseColumnDefs(trimmed);

        _logger.LogDebug("INSERT BULK parsed: Table={Table}, Columns={Count}",
            _pendingTableName, _pendingColumnMappings?.Count ?? 0);

        return true;
    }

    /// <summary>
    /// Handles the BulkLoadBCP (0x07) payload: parses COLMETADATA + ROW* + DONE,
    /// builds a DataTable, and uses SqlBulkCopy to write to the backend.
    /// Returns the number of rows inserted.
    /// </summary>
    public async Task<long> HandleBulkLoadAsync(
        byte[] payload, SqlConnection backendConnection, CancellationToken ct)
    {
        string tableName = _pendingTableName
            ?? throw new InvalidOperationException("BulkLoad received without a preceding INSERT BULK statement");
        var columnMappings = _pendingColumnMappings;

        _pendingTableName = null;
        _pendingColumnMappings = null;

        int offset = 0;

        // Parse COLMETADATA token
        if (offset >= payload.Length || payload[offset] != TdsConstants.TokenColMetadata)
            throw new InvalidOperationException(
                $"BulkLoad: expected COLMETADATA (0x81), got 0x{(offset < payload.Length ? payload[offset] : 0):X2}");

        offset++;
        ushort colCount = BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(offset));
        offset += 2;

        _logger.LogDebug("BulkLoad COLMETADATA: {Count} columns", colCount);

        var colMetas = new BulkColMeta[colCount];
        for (int c = 0; c < colCount; c++)
        {
            colMetas[c] = ReadColumnMeta(payload, ref offset);
            _logger.LogDebug("  Col[{Idx}]: Name={Name} TdsType=0x{Type:X2} MaxLen={MaxLen}",
                c, colMetas[c].Name, colMetas[c].TypeId, colMetas[c].MaxLength);
        }

        var dt = BuildDataTable(colMetas);

        long rowCount = 0;
        while (offset < payload.Length)
        {
            byte token = payload[offset];
            if (token == TdsConstants.TokenRow)
            {
                offset++;
                var row = ReadRow(payload, ref offset, colMetas);
                dt.Rows.Add(row);
                rowCount++;
            }
            else if (token == TdsConstants.TokenDone || token == TdsConstants.TokenDoneProc
                     || token == TdsConstants.TokenDoneInProc)
            {
                break;
            }
            else
            {
                _logger.LogWarning("BulkLoad: unexpected token 0x{Token:X2} at offset {Offset}, stopping row parse",
                    token, offset);
                break;
            }
        }

        _logger.LogDebug("BulkLoad: parsed {RowCount} rows for table {Table}", rowCount, tableName);

        using var bcp = new SqlBulkCopy(backendConnection);

        bcp.DestinationTableName = tableName;

        if (columnMappings != null)
        {
            for (int i = 0; i < Math.Min(columnMappings.Count, colMetas.Length); i++)
            {
                bcp.ColumnMappings.Add(i, columnMappings[i].Name);
            }
        }
        else
        {
            for (int i = 0; i < colMetas.Length; i++)
            {
                if (!string.IsNullOrEmpty(colMetas[i].Name))
                    bcp.ColumnMappings.Add(i, colMetas[i].Name);
            }
        }

        await bcp.WriteToServerAsync(dt, ct);

        _logger.LogInformation("BulkLoad: inserted {RowCount} rows into {Table}", rowCount, tableName);
        return rowCount;
    }

    private static string ParseTableName(string sql)
    {
        // "INSERT BULK [schema].[table] (...)" or "INSERT BULK table (...)"
        int startIdx = "INSERT BULK".Length;
        while (startIdx < sql.Length && char.IsWhiteSpace(sql[startIdx]))
            startIdx++;

        int endIdx = startIdx;

        if (endIdx < sql.Length && sql[endIdx] == '[')
        {
            // Bracketed identifier — may have [schema].[table]
            while (endIdx < sql.Length && sql[endIdx] != '(')
            {
                if (char.IsWhiteSpace(sql[endIdx]))
                    break;
                endIdx++;
            }
        }
        else
        {
            while (endIdx < sql.Length && !char.IsWhiteSpace(sql[endIdx]) && sql[endIdx] != '(')
                endIdx++;
        }

        return sql[startIdx..endIdx].Trim();
    }

    private static List<BulkColumnMapping>? ParseColumnDefs(string sql)
    {
        int parenStart = sql.IndexOf('(');
        if (parenStart < 0)
            return null;

        int depth = 0;
        int parenEnd = -1;
        for (int i = parenStart; i < sql.Length; i++)
        {
            if (sql[i] == '(') depth++;
            else if (sql[i] == ')') { depth--; if (depth == 0) { parenEnd = i; break; } }
        }
        if (parenEnd < 0)
            return null;

        string colDefs = sql[(parenStart + 1)..parenEnd];
        var mappings = new List<BulkColumnMapping>();

        foreach (var colDef in colDefs.Split(','))
        {
            var parts = colDef.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length >= 1)
            {
                string name = parts[0].Trim('[', ']');
                mappings.Add(new BulkColumnMapping(name));
            }
        }

        return mappings;
    }

    private BulkColMeta ReadColumnMeta(byte[] data, ref int offset)
    {
        uint userType = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset));
        offset += 4;

        ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
        offset += 2;

        byte typeId = data[offset++];

        int maxLength = 0;
        byte precision = 0;
        byte scale = 0;
        bool hasCollation = false;

        switch (typeId)
        {
            // Fixed-length types with no length byte
            case 0x30: // TinyInt (INT1TYPE)
                maxLength = 1;
                break;
            case 0x32: // Bit (BITTYPE)
                maxLength = 1;
                break;
            case 0x34: // SmallInt (INT2TYPE)
                maxLength = 2;
                break;
            case 0x38: // Int (INT4TYPE)
                maxLength = 4;
                break;
            case 0x3B: // Real (FLT4TYPE)
                maxLength = 4;
                break;
            case 0x3E: // Float (FLT8TYPE)
                maxLength = 8;
                break;
            case 0x7A: // SmallMoney (MONEY4TYPE)
                maxLength = 4;
                break;
            case 0x3C: // Money (MONEYTYPE)
                maxLength = 8;
                break;
            case 0x3D: // DateTime (DATETIM4TYPE)
                maxLength = 4;
                break;
            case 0x3F: // DateTime (DATETIMTYPE)
                maxLength = 8;
                break;

            // Nullable fixed-len types with a 1-byte maxlen
            case TdsConstants.TypeIntN:
            case TdsConstants.TypeBitN:
            case TdsConstants.TypeFltN:
            case TdsConstants.TypeMoneyN:
            case TdsConstants.TypeDateTimeN:
            case TdsConstants.TypeGuid:
                maxLength = data[offset++];
                break;

            case TdsConstants.TypeNumericN:
                maxLength = data[offset++];
                precision = data[offset++];
                scale = data[offset++];
                break;

            case TdsConstants.TypeDateN:
                maxLength = 3;
                break;

            case TdsConstants.TypeTimeN:
            case TdsConstants.TypeDateTime2N:
            case TdsConstants.TypeDateTimeOffsetN:
                scale = data[offset++];
                maxLength = scale switch { <= 2 => 3, <= 4 => 4, _ => 5 };
                if (typeId == TdsConstants.TypeDateTime2N) maxLength += 3;
                if (typeId == TdsConstants.TypeDateTimeOffsetN) maxLength += 5;
                break;

            case TdsConstants.TypeNVarChar:
                maxLength = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                offset += 2;
                hasCollation = true;
                offset += 5;
                break;

            case TdsConstants.TypeBigVarChar:
                maxLength = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                offset += 2;
                hasCollation = true;
                offset += 5;
                break;

            case TdsConstants.TypeBigVarBin:
            case TdsConstants.TypeBigBinary:
                maxLength = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                offset += 2;
                break;

            case TdsConstants.TypeNText:
                maxLength = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                hasCollation = true;
                offset += 5;
                break;

            case TdsConstants.TypeText:
                maxLength = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                hasCollation = true;
                offset += 5;
                break;

            case TdsConstants.TypeImage:
                maxLength = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                break;

            default:
                _logger.LogWarning("BulkLoad: unrecognized type 0x{Type:X2} in COLMETADATA, guessing 1-byte maxlen", typeId);
                maxLength = data[offset++];
                break;
        }

        // For TEXT/NTEXT/IMAGE, skip the table name (B_VARCHAR)
        if (typeId == TdsConstants.TypeNText || typeId == TdsConstants.TypeText || typeId == TdsConstants.TypeImage)
        {
            if (offset < data.Length)
            {
                // Table name parts count
                byte numParts = data[offset++];
                for (int p = 0; p < numParts; p++)
                {
                    ushort partLen = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                    offset += 2;
                    offset += partLen * 2;
                }
            }
        }

        // Column name
        byte nameLen = data[offset++];
        string name = "";
        if (nameLen > 0)
        {
            name = Encoding.Unicode.GetString(data, offset, nameLen * 2);
            offset += nameLen * 2;
        }

        return new BulkColMeta(typeId, maxLength, precision, scale, hasCollation, name);
    }

    private static DataTable BuildDataTable(BulkColMeta[] metas)
    {
        var dt = new DataTable();
        for (int c = 0; c < metas.Length; c++)
        {
            Type clrType = GetClrType(metas[c]);
            dt.Columns.Add(metas[c].Name, clrType);
        }
        return dt;
    }

    private static Type GetClrType(BulkColMeta meta)
    {
        return meta.TypeId switch
        {
            0x30 => typeof(byte),    // TinyInt
            0x34 => typeof(short),   // SmallInt
            0x38 => typeof(int),     // Int
            0x32 => typeof(bool),    // Bit
            0x3B => typeof(float),   // Real
            0x3E => typeof(double),  // Float
            0x3C or 0x7A => typeof(decimal), // Money/SmallMoney
            0x3D or 0x3F => typeof(DateTime), // SmallDateTime/DateTime

            TdsConstants.TypeIntN => meta.MaxLength switch
            {
                1 => typeof(byte),
                2 => typeof(short),
                8 => typeof(long),
                _ => typeof(int),
            },
            TdsConstants.TypeBitN => typeof(bool),
            TdsConstants.TypeFltN => meta.MaxLength <= 4 ? typeof(float) : typeof(double),
            TdsConstants.TypeMoneyN => typeof(decimal),
            TdsConstants.TypeDateTimeN => typeof(DateTime),
            TdsConstants.TypeNumericN => typeof(decimal),
            TdsConstants.TypeGuid => typeof(Guid),
            TdsConstants.TypeDateN => typeof(DateTime),
            TdsConstants.TypeTimeN => typeof(TimeSpan),
            TdsConstants.TypeDateTime2N => typeof(DateTime),
            TdsConstants.TypeDateTimeOffsetN => typeof(DateTimeOffset),
            TdsConstants.TypeNVarChar or TdsConstants.TypeBigVarChar
                or TdsConstants.TypeNText or TdsConstants.TypeText => typeof(string),
            TdsConstants.TypeBigVarBin or TdsConstants.TypeBigBinary
                or TdsConstants.TypeImage => typeof(byte[]),
            _ => typeof(object),
        };
    }

    private object[] ReadRow(byte[] data, ref int offset, BulkColMeta[] metas)
    {
        var row = new object[metas.Length];
        for (int c = 0; c < metas.Length; c++)
        {
            row[c] = ReadColumnValue(data, ref offset, metas[c]);
        }
        return row;
    }

    private object ReadColumnValue(byte[] data, ref int offset, BulkColMeta meta)
    {
        switch (meta.TypeId)
        {
            // Fixed-length types (no length prefix in ROW data)
            case 0x30: // TinyInt
                return data[offset++];
            case 0x32: // Bit
                return data[offset++] != 0;
            case 0x34: // SmallInt
            {
                short v = BinaryPrimitives.ReadInt16LittleEndian(data.AsSpan(offset));
                offset += 2;
                return v;
            }
            case 0x38: // Int
            {
                int v = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                return v;
            }
            case 0x3B: // Real
            {
                float v = BitConverter.ToSingle(data, offset);
                offset += 4;
                return v;
            }
            case 0x3E: // Float
            {
                double v = BitConverter.ToDouble(data, offset);
                offset += 8;
                return v;
            }
            case 0x3C: // Money
            {
                int hi = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                int lo = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset + 4));
                offset += 8;
                long combined = ((long)hi << 32) | (uint)lo;
                return combined / 10000m;
            }
            case 0x7A: // SmallMoney
            {
                int v = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                return v / 10000m;
            }
            case 0x3D: // SmallDateTime
            {
                ushort days = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                ushort mins = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset + 2));
                offset += 4;
                return new DateTime(1900, 1, 1).AddDays(days).AddMinutes(mins);
            }
            case 0x3F: // DateTime
            {
                int days = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                int ticks = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset + 4));
                offset += 8;
                return new DateTime(1900, 1, 1).AddDays(days).AddSeconds(ticks / 300.0);
            }

            // Nullable fixed-len types (1-byte length prefix)
            case TdsConstants.TypeIntN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                object val = len switch
                {
                    1 => (object)data[offset],
                    2 => (object)BinaryPrimitives.ReadInt16LittleEndian(data.AsSpan(offset)),
                    4 => (object)BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset)),
                    8 => (object)BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(offset)),
                    _ => (object)BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset)),
                };
                offset += len;
                return val;
            }

            case TdsConstants.TypeBitN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                bool v = data[offset++] != 0;
                return v;
            }

            case TdsConstants.TypeFltN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                if (len == 4)
                {
                    float v = BitConverter.ToSingle(data, offset);
                    offset += 4;
                    return v;
                }
                double d = BitConverter.ToDouble(data, offset);
                offset += 8;
                return d;
            }

            case TdsConstants.TypeMoneyN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                if (len == 4)
                {
                    int v = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                    offset += 4;
                    return v / 10000m;
                }
                int mhi = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                int mlo = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset + 4));
                offset += 8;
                long combined = ((long)mhi << 32) | (uint)mlo;
                return combined / 10000m;
            }

            case TdsConstants.TypeDateTimeN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                if (len == 4)
                {
                    ushort days = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
                    ushort mins = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset + 2));
                    offset += 4;
                    return new DateTime(1900, 1, 1).AddDays(days).AddMinutes(mins);
                }
                int ddays = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
                int dticks = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset + 4));
                offset += 8;
                return new DateTime(1900, 1, 1).AddDays(ddays).AddSeconds(dticks / 300.0);
            }

            case TdsConstants.TypeGuid:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                var guid = new Guid(data.AsSpan(offset, 16));
                offset += 16;
                return guid;
            }

            case TdsConstants.TypeNumericN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                byte sign = data[offset];
                byte[] intBytes = new byte[len - 1];
                Array.Copy(data, offset + 1, intBytes, 0, len - 1);
                offset += len;

                ulong lo = 0, hi = 0;
                for (int i = Math.Min(intBytes.Length, 8) - 1; i >= 0; i--)
                    lo = (lo << 8) | intBytes[i];
                for (int i = Math.Min(intBytes.Length, 16) - 1; i >= 8; i--)
                    hi = (hi << 8) | intBytes[i];

                decimal result = (decimal)lo;
                if (hi != 0)
                    result += (decimal)hi * (1UL << 32) * (1UL << 32);
                for (int i = 0; i < meta.Scale; i++)
                    result /= 10m;
                if (sign == 0)
                    result = -result;
                return result;
            }

            case TdsConstants.TypeDateN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                int days = data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16);
                offset += 3;
                return new DateTime(1, 1, 1).AddDays(days);
            }

            case TdsConstants.TypeTimeN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                long scaledVal = ReadScaledInt(data, offset, len);
                offset += len;
                double factor = Math.Pow(10, meta.Scale);
                long ticks = (long)(scaledVal / factor * TimeSpan.TicksPerSecond);
                return new TimeSpan(ticks);
            }

            case TdsConstants.TypeDateTime2N:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                int timeBytes = len - 3;
                long scaledTime = ReadScaledInt(data, offset, timeBytes);
                int days = data[offset + timeBytes]
                         | (data[offset + timeBytes + 1] << 8)
                         | (data[offset + timeBytes + 2] << 16);
                offset += len;
                double factor = Math.Pow(10, meta.Scale);
                long timeTicks = (long)(scaledTime / factor * TimeSpan.TicksPerSecond);
                return new DateTime(1, 1, 1).AddDays(days).AddTicks(timeTicks);
            }

            case TdsConstants.TypeDateTimeOffsetN:
            {
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                int timeBytes = len - 5;
                long scaledTime = ReadScaledInt(data, offset, timeBytes);
                int dayStart = offset + timeBytes;
                int days = data[dayStart] | (data[dayStart + 1] << 8) | (data[dayStart + 2] << 16);
                short offsetMins = BinaryPrimitives.ReadInt16LittleEndian(data.AsSpan(dayStart + 3));
                offset += len;
                double factor = Math.Pow(10, meta.Scale);
                long timeTicks = (long)(scaledTime / factor * TimeSpan.TicksPerSecond);
                var dt = new DateTime(1, 1, 1).AddDays(days).AddTicks(timeTicks);
                return new DateTimeOffset(dt, TimeSpan.FromMinutes(offsetMins));
            }

            case TdsConstants.TypeNVarChar:
                return ReadVarLenString(data, ref offset, meta.MaxLength, Encoding.Unicode);

            case TdsConstants.TypeBigVarChar:
                return ReadVarLenString(data, ref offset, meta.MaxLength, Encoding.UTF8);

            case TdsConstants.TypeBigVarBin:
            case TdsConstants.TypeBigBinary:
                return ReadVarLenBinary(data, ref offset, meta.MaxLength);

            case TdsConstants.TypeNText:
                return ReadTextValue(data, ref offset, Encoding.Unicode);

            case TdsConstants.TypeText:
                return ReadTextValue(data, ref offset, Encoding.UTF8);

            case TdsConstants.TypeImage:
                return ReadImageValue(data, ref offset);

            default:
            {
                _logger.LogWarning("BulkLoad: unhandled column type 0x{Type:X2}, reading as nullable fixed-len", meta.TypeId);
                byte len = data[offset++];
                if (len == 0) return DBNull.Value;
                byte[] val = new byte[len];
                Array.Copy(data, offset, val, 0, len);
                offset += len;
                return val;
            }
        }
    }

    private static object ReadVarLenString(byte[] data, ref int offset, int maxLength, Encoding encoding)
    {
        if (maxLength == 0xFFFF)
        {
            // PLP mode
            ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset));
            offset += 8;
            if (totalLen == 0xFFFFFFFFFFFFFFFF) return DBNull.Value;

            var sb = new StringBuilder();
            while (offset < data.Length)
            {
                uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                if (chunkLen == 0) break;
                sb.Append(encoding.GetString(data, offset, (int)chunkLen));
                offset += (int)chunkLen;
            }
            return sb.ToString();
        }

        ushort len = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
        offset += 2;
        if (len == 0xFFFF) return DBNull.Value;
        string val = encoding.GetString(data, offset, len);
        offset += len;
        return val;
    }

    private static object ReadVarLenBinary(byte[] data, ref int offset, int maxLength)
    {
        if (maxLength == 0xFFFF)
        {
            ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset));
            offset += 8;
            if (totalLen == 0xFFFFFFFFFFFFFFFF) return DBNull.Value;

            using var ms = new MemoryStream();
            while (offset < data.Length)
            {
                uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset));
                offset += 4;
                if (chunkLen == 0) break;
                ms.Write(data, offset, (int)chunkLen);
                offset += (int)chunkLen;
            }
            return ms.ToArray();
        }

        ushort len = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
        offset += 2;
        if (len == 0xFFFF) return DBNull.Value;
        byte[] val = new byte[len];
        Array.Copy(data, offset, val, 0, len);
        offset += len;
        return val;
    }

    private static object ReadTextValue(byte[] data, ref int offset, Encoding encoding)
    {
        byte textPtrLen = data[offset++];
        if (textPtrLen == 0) return DBNull.Value;
        offset += textPtrLen; // skip textptr
        offset += 8;          // skip timestamp (8 bytes)
        int dataLen = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
        offset += 4;
        string val = encoding.GetString(data, offset, dataLen);
        offset += dataLen;
        return val;
    }

    private static object ReadImageValue(byte[] data, ref int offset)
    {
        byte textPtrLen = data[offset++];
        if (textPtrLen == 0) return DBNull.Value;
        offset += textPtrLen; // skip textptr
        offset += 8;          // skip timestamp
        int dataLen = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(offset));
        offset += 4;
        byte[] val = new byte[dataLen];
        Array.Copy(data, offset, val, 0, dataLen);
        offset += dataLen;
        return val;
    }

    private static long ReadScaledInt(byte[] data, int offset, int byteCount)
    {
        long val = 0;
        for (int i = byteCount - 1; i >= 0; i--)
            val = (val << 8) | data[offset + i];
        return val;
    }
}

internal sealed record BulkColMeta(
    byte TypeId,
    int MaxLength,
    byte Precision,
    byte Scale,
    bool HasCollation,
    string Name);

internal sealed record BulkColumnMapping(string Name);
