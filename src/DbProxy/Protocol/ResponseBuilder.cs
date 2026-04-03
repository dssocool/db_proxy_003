using System.Text;
using DbProxy.Tds;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

/// <summary>
/// Translates ADO.NET SqlDataReader results into TDS token byte streams
/// (COLMETADATA + ROW + DONE) that can be sent to the client.
/// </summary>
public sealed class ResponseBuilder
{
    private readonly ILogger _logger;

    public ResponseBuilder(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Executes a SQL command against the backend and builds the complete TDS response bytes.
    /// Handles both result-returning queries (SELECT) and non-query statements (INSERT/UPDATE/DELETE/DDL).
    /// </summary>
    public async Task<byte[]> BuildResponseFromCommandAsync(SqlCommand cmd, CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);

        try
        {
            using var reader = await cmd.ExecuteReaderAsync(ct);
            int resultSetIndex = 0;

            do
            {
                resultSetIndex++;
                bool hasMoreResults = false;

                if (reader.FieldCount > 0)
                {
                    var colInfos = BuildColumnInfos(reader);
                    _logger.LogDebug("ResultSet #{Idx}: {ColCount} columns", resultSetIndex, colInfos.Length);
                    for (int c = 0; c < colInfos.Length; c++)
                        _logger.LogDebug("  Col[{Idx}]: {Name} ({TypeName}, TdsType=0x{TdsType:X2}, MaxLen={MaxLen})",
                            c, colInfos[c].Name, colInfos[c].DataTypeName, colInfos[c].TdsTypeId, colInfos[c].MaxLength);

                    WriteColMetadata(bw, colInfos);

                    long rowCount = 0;
                    while (await reader.ReadAsync(ct))
                    {
                        WriteRow(bw, reader, colInfos);
                        rowCount++;
                    }
                    _logger.LogDebug("ResultSet #{Idx}: {RowCount} rows written", resultSetIndex, rowCount);

                    hasMoreResults = await reader.NextResultAsync(ct);

                    ushort doneStatus = (ushort)(TdsConstants.DoneCount
                        | (hasMoreResults ? TdsConstants.DoneMore : 0));
                    LoginHandler.WriteDoneToken(bw, doneStatus, rowCount);
                    _logger.LogDebug("ResultSet #{Idx}: DONE token status=0x{Status:X4} rowCount={RowCount} hasMore={More}",
                        resultSetIndex, doneStatus, rowCount, hasMoreResults);
                }
                else
                {
                    int affected = reader.RecordsAffected;
                    _logger.LogDebug("ResultSet #{Idx}: non-query, RecordsAffected={Affected}", resultSetIndex, affected);

                    hasMoreResults = await reader.NextResultAsync(ct);

                    ushort status = (ushort)(
                        (hasMoreResults ? TdsConstants.DoneMore : 0)
                        | (affected >= 0 ? TdsConstants.DoneCount : 0));
                    LoginHandler.WriteDoneToken(bw, status, affected >= 0 ? affected : 0);
                    _logger.LogDebug("ResultSet #{Idx}: DONE token status=0x{Status:X4} hasMore={More}",
                        resultSetIndex, status, hasMoreResults);
                }

                if (!hasMoreResults)
                    break;

            } while (true);

            LoginHandler.WriteDoneToken(bw, TdsConstants.DoneFinal, 0);
            _logger.LogDebug("Final DONE token written (status=0x{Status:X4}). Total result sets: {Count}",
                TdsConstants.DoneFinal, resultSetIndex);
        }
        catch (SqlException ex)
        {
            _logger.LogError(ex, "Backend SQL error: {Msg}", ex.Message);
            LoginHandler.WriteErrorToken(bw, ex.Number, ex.Class, ex.State,
                ex.Message, "DbProxy", ex.Procedure ?? "", ex.LineNumber);
            LoginHandler.WriteDoneToken(bw, (ushort)(TdsConstants.DoneError | TdsConstants.DoneFinal), 0);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error executing query");
            LoginHandler.WriteErrorToken(bw, 50000, 16, 1,
                ex.Message, "DbProxy", "", 0);
            LoginHandler.WriteDoneToken(bw, (ushort)(TdsConstants.DoneError | TdsConstants.DoneFinal), 0);
        }

        return ms.ToArray();
    }

    private sealed class ColumnInfo
    {
        public string Name { get; init; } = "";
        public Type ClrType { get; init; } = typeof(object);
        public string DataTypeName { get; init; } = "";
        public byte TdsTypeId { get; init; }
        public int MaxLength { get; init; }
        public byte Precision { get; init; }
        public byte Scale { get; init; }
        public bool IsNullable { get; init; }
        public bool IsVarLen { get; init; }
        public bool NeedsCollation { get; init; }
    }

    private ColumnInfo[] BuildColumnInfos(SqlDataReader reader)
    {
        var cols = new ColumnInfo[reader.FieldCount];
        var schemaTable = reader.GetSchemaTable();

        for (int i = 0; i < reader.FieldCount; i++)
        {
            var clrType = reader.GetFieldType(i)!;
            var typeName = reader.GetDataTypeName(i).ToLowerInvariant();
            bool isNullable = true;
            int columnSize = 0;
            byte precision = 0;
            byte scale = 0;

            if (schemaTable != null && i < schemaTable.Rows.Count)
            {
                var row = schemaTable.Rows[i];
                if (row["AllowDBNull"] is bool n) isNullable = n;
                if (row["ColumnSize"] is int cs) columnSize = cs;
                if (row["NumericPrecision"] is short np) precision = (byte)np;
                else if (row["NumericPrecision"] is int np2) precision = (byte)np2;
                if (row["NumericScale"] is short ns) scale = (byte)ns;
                else if (row["NumericScale"] is int ns2) scale = (byte)ns2;
            }

            var (tdsType, maxLen, isVarLen, needsCollation) = MapToTdsType(typeName, clrType, columnSize, precision);

            cols[i] = new ColumnInfo
            {
                Name = reader.GetName(i),
                ClrType = clrType,
                DataTypeName = typeName,
                TdsTypeId = tdsType,
                MaxLength = maxLen,
                Precision = precision,
                Scale = scale,
                IsNullable = isNullable,
                IsVarLen = isVarLen,
                NeedsCollation = needsCollation,
            };
        }

        return cols;
    }

    private static (byte tdsType, int maxLen, bool isVarLen, bool needsCollation) MapToTdsType(
        string typeName, Type clrType, int columnSize, byte precision)
    {
        return typeName switch
        {
            "int" => (TdsConstants.TypeIntN, 4, false, false),
            "bigint" => (TdsConstants.TypeIntN, 8, false, false),
            "smallint" => (TdsConstants.TypeIntN, 2, false, false),
            "tinyint" => (TdsConstants.TypeIntN, 1, false, false),
            "bit" => (TdsConstants.TypeBitN, 1, false, false),
            "float" => (TdsConstants.TypeFltN, 8, false, false),
            "real" => (TdsConstants.TypeFltN, 4, false, false),
            "decimal" or "numeric" => (TdsConstants.TypeNumericN, 17, false, false),
            "money" => (TdsConstants.TypeMoneyN, 8, false, false),
            "smallmoney" => (TdsConstants.TypeMoneyN, 4, false, false),
            "datetime" => (TdsConstants.TypeDateTimeN, 8, false, false),
            "smalldatetime" => (TdsConstants.TypeDateTimeN, 4, false, false),
            "date" => (TdsConstants.TypeDateN, 3, false, false),
            "time" => (TdsConstants.TypeTimeN, 5, false, false),
            "datetime2" => (TdsConstants.TypeDateTime2N, 8, false, false),
            "datetimeoffset" => (TdsConstants.TypeDateTimeOffsetN, 10, false, false),
            "uniqueidentifier" => (TdsConstants.TypeGuid, 16, false, false),
            "nvarchar" or "nchar" or "sysname" =>
                (TdsConstants.TypeNVarChar, columnSize > 0 && columnSize <= 4000 ? columnSize * 2 : 8000, true, true),
            "varchar" or "char" =>
                (TdsConstants.TypeBigVarChar, columnSize > 0 && columnSize <= 8000 ? columnSize : 8000, true, true),
            "ntext" => (TdsConstants.TypeNVarChar, 8000, true, true),
            "text" => (TdsConstants.TypeBigVarChar, 8000, true, true),
            "varbinary" or "binary" or "timestamp" =>
                (TdsConstants.TypeBigVarBin, columnSize > 0 && columnSize <= 8000 ? columnSize : 8000, true, false),
            "image" => (TdsConstants.TypeBigVarBin, 8000, true, false),
            "xml" => (TdsConstants.TypeNVarChar, 8000, true, true),
            _ => MapFromClrType(clrType),
        };
    }

    private static (byte tdsType, int maxLen, bool isVarLen, bool needsCollation) MapFromClrType(Type clrType)
    {
        if (clrType == typeof(int)) return (TdsConstants.TypeIntN, 4, false, false);
        if (clrType == typeof(long)) return (TdsConstants.TypeIntN, 8, false, false);
        if (clrType == typeof(short)) return (TdsConstants.TypeIntN, 2, false, false);
        if (clrType == typeof(byte)) return (TdsConstants.TypeIntN, 1, false, false);
        if (clrType == typeof(bool)) return (TdsConstants.TypeBitN, 1, false, false);
        if (clrType == typeof(double)) return (TdsConstants.TypeFltN, 8, false, false);
        if (clrType == typeof(float)) return (TdsConstants.TypeFltN, 4, false, false);
        if (clrType == typeof(decimal)) return (TdsConstants.TypeNumericN, 17, false, false);
        if (clrType == typeof(Guid)) return (TdsConstants.TypeGuid, 16, false, false);
        if (clrType == typeof(byte[])) return (TdsConstants.TypeBigVarBin, 8000, true, false);
        if (clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset))
            return (TdsConstants.TypeDateTimeN, 8, false, false);
        // Default: send as NVARCHAR
        return (TdsConstants.TypeNVarChar, 8000, true, true);
    }

    private static void WriteColMetadata(BinaryWriter bw, ColumnInfo[] columns)
    {
        bw.Write(TdsConstants.TokenColMetadata);
        bw.Write((ushort)columns.Length);

        foreach (var col in columns)
        {
            // UserType (ULONG for TDS 7.2+)
            bw.Write((uint)0);

            // Flags (2 bytes): bit 0 = nullable
            ushort flags = (ushort)(col.IsNullable ? 0x0001 : 0x0000);
            bw.Write(flags);

            // TYPE_INFO
            bw.Write(col.TdsTypeId);

            switch (col.TdsTypeId)
            {
                case TdsConstants.TypeIntN:
                case TdsConstants.TypeFltN:
                case TdsConstants.TypeMoneyN:
                case TdsConstants.TypeDateTimeN:
                case TdsConstants.TypeBitN:
                case TdsConstants.TypeGuid:
                    bw.Write((byte)col.MaxLength);
                    break;

                case TdsConstants.TypeNumericN:
                    bw.Write((byte)col.MaxLength);
                    bw.Write(col.Precision == 0 ? (byte)18 : col.Precision);
                    bw.Write(col.Scale);
                    break;

                case TdsConstants.TypeDateN:
                    break;

                case TdsConstants.TypeTimeN:
                case TdsConstants.TypeDateTime2N:
                case TdsConstants.TypeDateTimeOffsetN:
                    bw.Write(col.Scale == 0 ? (byte)7 : col.Scale);
                    break;

                case TdsConstants.TypeNVarChar:
                    bw.Write((ushort)col.MaxLength);
                    bw.Write(TdsConstants.DefaultCollation);
                    break;

                case TdsConstants.TypeBigVarChar:
                    bw.Write((ushort)col.MaxLength);
                    bw.Write(TdsConstants.DefaultCollation);
                    break;

                case TdsConstants.TypeBigVarBin:
                    bw.Write((ushort)col.MaxLength);
                    break;

                default:
                    bw.Write((byte)col.MaxLength);
                    break;
            }

            // ColName: B_VARCHAR (length byte in chars, then UTF-16LE)
            bw.Write((byte)col.Name.Length);
            bw.Write(Encoding.Unicode.GetBytes(col.Name));
        }
    }

    private void WriteRow(BinaryWriter bw, SqlDataReader reader, ColumnInfo[] columns)
    {
        bw.Write(TdsConstants.TokenRow);

        for (int i = 0; i < columns.Length; i++)
        {
            var col = columns[i];

            if (reader.IsDBNull(i))
            {
                WriteNullValue(bw, col);
                continue;
            }

            WriteColumnValue(bw, reader, i, col);
        }
    }

    private static void WriteNullValue(BinaryWriter bw, ColumnInfo col)
    {
        switch (col.TdsTypeId)
        {
            case TdsConstants.TypeIntN:
            case TdsConstants.TypeFltN:
            case TdsConstants.TypeMoneyN:
            case TdsConstants.TypeDateTimeN:
            case TdsConstants.TypeBitN:
            case TdsConstants.TypeGuid:
            case TdsConstants.TypeNumericN:
                bw.Write((byte)0); // zero-length = NULL for nullable fixed types
                break;

            case TdsConstants.TypeDateN:
                bw.Write((byte)0);
                break;

            case TdsConstants.TypeTimeN:
            case TdsConstants.TypeDateTime2N:
            case TdsConstants.TypeDateTimeOffsetN:
                bw.Write((byte)0);
                break;

            case TdsConstants.TypeNVarChar:
            case TdsConstants.TypeBigVarChar:
            case TdsConstants.TypeBigVarBin:
                bw.Write(unchecked((ushort)0xFFFF)); // PLP_NULL / CHARBIN_NULL
                break;

            default:
                bw.Write((byte)0);
                break;
        }
    }

    private void WriteColumnValue(BinaryWriter bw, SqlDataReader reader, int ordinal, ColumnInfo col)
    {
        try
        {
            switch (col.TdsTypeId)
            {
                case TdsConstants.TypeIntN:
                    WriteIntNValue(bw, reader, ordinal, col.MaxLength);
                    break;

                case TdsConstants.TypeBitN:
                    bw.Write((byte)1);
                    bw.Write(reader.GetBoolean(ordinal) ? (byte)1 : (byte)0);
                    break;

                case TdsConstants.TypeFltN:
                    if (col.MaxLength == 4)
                    {
                        bw.Write((byte)4);
                        bw.Write(reader.GetFloat(ordinal));
                    }
                    else
                    {
                        bw.Write((byte)8);
                        bw.Write(reader.GetDouble(ordinal));
                    }
                    break;

                case TdsConstants.TypeMoneyN:
                    WriteMoneyValue(bw, reader.GetDecimal(ordinal), col.MaxLength);
                    break;

                case TdsConstants.TypeDateTimeN:
                    WriteDateTimeNValue(bw, reader.GetDateTime(ordinal), col.MaxLength);
                    break;

                case TdsConstants.TypeNumericN:
                    WriteNumericValue(bw, reader.GetDecimal(ordinal), col.Precision, col.Scale);
                    break;

                case TdsConstants.TypeGuid:
                    bw.Write((byte)16);
                    bw.Write(reader.GetGuid(ordinal).ToByteArray());
                    break;

                case TdsConstants.TypeDateN:
                    WriteDateValue(bw, reader.GetDateTime(ordinal));
                    break;

                case TdsConstants.TypeTimeN:
                    WriteTimeValue(bw, reader.GetTimeSpan(ordinal), col.Scale == 0 ? (byte)7 : col.Scale);
                    break;

                case TdsConstants.TypeDateTime2N:
                    WriteDateTime2Value(bw, reader.GetDateTime(ordinal), col.Scale == 0 ? (byte)7 : col.Scale);
                    break;

                case TdsConstants.TypeDateTimeOffsetN:
                    WriteDateTimeOffsetValue(bw, reader.GetDateTimeOffset(ordinal), col.Scale == 0 ? (byte)7 : col.Scale);
                    break;

                case TdsConstants.TypeNVarChar:
                    WriteNVarCharValue(bw, reader.GetValue(ordinal)?.ToString() ?? "");
                    break;

                case TdsConstants.TypeBigVarChar:
                    WriteVarCharValue(bw, reader.GetValue(ordinal)?.ToString() ?? "");
                    break;

                case TdsConstants.TypeBigVarBin:
                    WriteBinaryValue(bw, reader, ordinal);
                    break;

                default:
                    WriteNVarCharValue(bw, reader.GetValue(ordinal)?.ToString() ?? "");
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error writing column {Col} (type {Type}), sending as NVARCHAR",
                col.Name, col.DataTypeName);
            try
            {
                var value = reader.GetValue(ordinal)?.ToString() ?? "";
                WriteNVarCharValue(bw, value);
            }
            catch
            {
                WriteNullValue(bw, col);
            }
        }
    }

    private static void WriteIntNValue(BinaryWriter bw, SqlDataReader reader, int ordinal, int maxLen)
    {
        switch (maxLen)
        {
            case 1:
                bw.Write((byte)1);
                bw.Write(reader.GetByte(ordinal));
                break;
            case 2:
                bw.Write((byte)2);
                bw.Write(reader.GetInt16(ordinal));
                break;
            case 4:
                bw.Write((byte)4);
                bw.Write(reader.GetInt32(ordinal));
                break;
            case 8:
                bw.Write((byte)8);
                bw.Write(reader.GetInt64(ordinal));
                break;
            default:
                bw.Write((byte)4);
                bw.Write(reader.GetInt32(ordinal));
                break;
        }
    }

    private static void WriteMoneyValue(BinaryWriter bw, decimal value, int maxLen)
    {
        if (maxLen == 4)
        {
            bw.Write((byte)4);
            bw.Write((int)(value * 10000m));
        }
        else
        {
            bw.Write((byte)8);
            long scaled = (long)(value * 10000m);
            int hi = (int)(scaled >> 32);
            int lo = (int)(scaled & 0xFFFFFFFF);
            bw.Write(hi);
            bw.Write(lo);
        }
    }

    private static void WriteDateTimeNValue(BinaryWriter bw, DateTime value, int maxLen)
    {
        if (maxLen == 4)
        {
            // smalldatetime: days since 1900-01-01 (ushort) + minutes since midnight (ushort)
            bw.Write((byte)4);
            int days = (value.Date - new DateTime(1900, 1, 1)).Days;
            int mins = value.Hour * 60 + value.Minute;
            bw.Write((ushort)days);
            bw.Write((ushort)mins);
        }
        else
        {
            // datetime: days since 1900-01-01 (int) + 300ths of second since midnight (int)
            bw.Write((byte)8);
            int days = (value.Date - new DateTime(1900, 1, 1)).Days;
            int ticks = (int)(value.TimeOfDay.TotalSeconds * 300.0 + 0.5);
            bw.Write(days);
            bw.Write(ticks);
        }
    }

    private static void WriteNumericValue(BinaryWriter bw, decimal value, byte precision, byte scale)
    {
        byte prec = precision == 0 ? (byte)18 : precision;

        // Scale the value
        decimal scaled = value;
        for (int i = 0; i < scale; i++)
            scaled *= 10m;

        byte sign = scaled >= 0 ? (byte)1 : (byte)0;
        if (scaled < 0) scaled = -scaled;

        var intVal = (BigInteger128)scaled;
        byte[] bytes = intVal.ToBytes();

        // Determine length based on precision
        int dataLen = prec switch
        {
            <= 9 => 4,
            <= 19 => 8,
            <= 28 => 12,
            _ => 16,
        };

        bw.Write((byte)(dataLen + 1)); // +1 for sign
        bw.Write(sign);

        // Write little-endian integer bytes, padded to dataLen
        byte[] padded = new byte[dataLen];
        Array.Copy(bytes, padded, Math.Min(bytes.Length, dataLen));
        bw.Write(padded);
    }

    private static void WriteDateValue(BinaryWriter bw, DateTime value)
    {
        bw.Write((byte)3);
        int days = (value.Date - new DateTime(1, 1, 1)).Days;
        bw.Write((byte)(days & 0xFF));
        bw.Write((byte)((days >> 8) & 0xFF));
        bw.Write((byte)((days >> 16) & 0xFF));
    }

    private static void WriteTimeValue(BinaryWriter bw, TimeSpan value, byte scale)
    {
        long ticks = value.Ticks;
        long scaledValue = ScaleTimeTicks(ticks, scale);
        int byteCount = TimeByteCount(scale);

        bw.Write((byte)byteCount);
        WriteScaledTime(bw, scaledValue, byteCount);
    }

    private static void WriteDateTime2Value(BinaryWriter bw, DateTime value, byte scale)
    {
        long ticks = value.TimeOfDay.Ticks;
        long scaledTime = ScaleTimeTicks(ticks, scale);
        int timeBytes = TimeByteCount(scale);
        int days = (value.Date - new DateTime(1, 1, 1)).Days;

        bw.Write((byte)(timeBytes + 3));
        WriteScaledTime(bw, scaledTime, timeBytes);
        bw.Write((byte)(days & 0xFF));
        bw.Write((byte)((days >> 8) & 0xFF));
        bw.Write((byte)((days >> 16) & 0xFF));
    }

    private static void WriteDateTimeOffsetValue(BinaryWriter bw, DateTimeOffset value, byte scale)
    {
        long ticks = value.TimeOfDay.Ticks;
        long scaledTime = ScaleTimeTicks(ticks, scale);
        int timeBytes = TimeByteCount(scale);
        int days = (value.Date - new DateTime(1, 1, 1)).Days;
        short offsetMinutes = (short)value.Offset.TotalMinutes;

        bw.Write((byte)(timeBytes + 5));
        WriteScaledTime(bw, scaledTime, timeBytes);
        bw.Write((byte)(days & 0xFF));
        bw.Write((byte)((days >> 8) & 0xFF));
        bw.Write((byte)((days >> 16) & 0xFF));
        bw.Write(offsetMinutes);
    }

    private static long ScaleTimeTicks(long ticks, byte scale)
    {
        // .NET ticks are 100ns. Scale to 10^(-scale) seconds.
        double seconds = ticks / 10_000_000.0;
        double factor = Math.Pow(10, scale);
        return (long)(seconds * factor + 0.5);
    }

    private static int TimeByteCount(byte scale) => scale switch
    {
        <= 2 => 3,
        <= 4 => 4,
        _ => 5,
    };

    private static void WriteScaledTime(BinaryWriter bw, long value, int byteCount)
    {
        for (int i = 0; i < byteCount; i++)
        {
            bw.Write((byte)(value & 0xFF));
            value >>= 8;
        }
    }

    private static void WriteNVarCharValue(BinaryWriter bw, string value)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(value);
        bw.Write((ushort)bytes.Length);
        bw.Write(bytes);
    }

    private static void WriteVarCharValue(BinaryWriter bw, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        bw.Write((ushort)bytes.Length);
        bw.Write(bytes);
    }

    private static void WriteBinaryValue(BinaryWriter bw, SqlDataReader reader, int ordinal)
    {
        long len = reader.GetBytes(ordinal, 0, null, 0, 0);
        byte[] buf = new byte[len];
        reader.GetBytes(ordinal, 0, buf, 0, (int)len);
        bw.Write((ushort)buf.Length);
        bw.Write(buf);
    }

    /// <summary>
    /// Minimal 128-bit unsigned integer for NUMERIC/DECIMAL TDS encoding.
    /// </summary>
    private readonly struct BigInteger128
    {
        private readonly ulong _lo;
        private readonly ulong _hi;

        private BigInteger128(ulong lo, ulong hi) { _lo = lo; _hi = hi; }

        public static explicit operator BigInteger128(decimal value)
        {
            int[] bits = decimal.GetBits(value);
            ulong lo = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
            ulong hi = (uint)bits[2];
            return new BigInteger128(lo, hi);
        }

        public byte[] ToBytes()
        {
            byte[] result = new byte[16];
            BitConverter.TryWriteBytes(result.AsSpan(0, 8), _lo);
            BitConverter.TryWriteBytes(result.AsSpan(8, 8), _hi);
            return result;
        }
    }
}
