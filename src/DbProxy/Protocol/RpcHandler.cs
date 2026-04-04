using System.Buffers.Binary;
using System.Data;
using System.Data.Common;
using System.Text;
using DbProxy.Tds;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

public sealed class RpcHandler
{
    private readonly ILogger _logger;
    private readonly ResponseBuilder _responseBuilder;
    private readonly Dictionary<int, PreparedStatement> _preparedStatements = new();
    private int _nextHandle = 1;

    public RpcHandler(ILogger logger, ResponseBuilder responseBuilder)
    {
        _logger = logger;
        _responseBuilder = responseBuilder;
    }

    public async Task<byte[]> HandleRpcAsync(byte[] payload, SqlConnection backendConnection, CancellationToken ct)
    {
        var rpc = ParseRpcRequest(payload);

        _logger.LogDebug("RPC: ProcId={ProcId} ProcName={ProcName} ParamCount={Count}",
            rpc.ProcId, rpc.ProcName ?? "(by id)", rpc.Parameters.Count);

        if (rpc.ProcId.HasValue)
        {
            return rpc.ProcId.Value switch
            {
                TdsConstants.SpExecuteSql => await HandleSpExecuteSqlAsync(rpc, backendConnection, ct),
                TdsConstants.SpPrepare => HandleSpPrepare(rpc),
                TdsConstants.SpExecute => await HandleSpExecuteAsync(rpc, backendConnection, ct),
                TdsConstants.SpPrepExec => await HandleSpPrepExecAsync(rpc, backendConnection, ct),
                TdsConstants.SpUnprepare => HandleSpUnprepare(rpc),
                _ => await HandleGenericRpcAsync(rpc, backendConnection, ct),
            };
        }

        return await HandleNamedProcAsync(rpc, backendConnection, ct);
    }

    private async Task<byte[]> HandleSpExecuteSqlAsync(ParsedRpc rpc, SqlConnection conn, CancellationToken ct)
    {
        if (rpc.Parameters.Count < 1)
            return BuildRpcError("sp_executesql requires at least a SQL parameter");

        string sql = rpc.Parameters[0].StringValue
            ?? throw new InvalidOperationException("sp_executesql: first parameter (SQL) is null");

        _logger.LogDebug("sp_executesql SQL: {Sql}", sql.Length > 500 ? sql[..500] + "..." : sql);

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 120;

        BindUserParameters(cmd, rpc.Parameters);

        return await _responseBuilder.BuildRpcResponseAsync(cmd, returnStatus: 0, ct: ct);
    }

    private byte[] HandleSpPrepare(ParsedRpc rpc)
    {
        if (rpc.Parameters.Count < 3)
            return BuildRpcError("sp_prepare requires handle, paramdef, and sql parameters");

        string? paramDef = rpc.Parameters[1].StringValue;
        string sql = rpc.Parameters[2].StringValue
            ?? throw new InvalidOperationException("sp_prepare: sql parameter is null");

        int handle = _nextHandle++;
        _preparedStatements[handle] = new PreparedStatement(sql, paramDef);

        _logger.LogDebug("sp_prepare: allocated handle {Handle} for SQL: {Sql}",
            handle, sql.Length > 200 ? sql[..200] + "..." : sql);

        return BuildPrepareResponse(handle);
    }

    private async Task<byte[]> HandleSpExecuteAsync(ParsedRpc rpc, SqlConnection conn, CancellationToken ct)
    {
        if (rpc.Parameters.Count < 1)
            return BuildRpcError("sp_execute requires at least a handle parameter");

        int handle = rpc.Parameters[0].IntValue
            ?? throw new InvalidOperationException("sp_execute: handle parameter is null");

        if (!_preparedStatements.TryGetValue(handle, out var prepared))
            return BuildRpcError($"sp_execute: invalid prepare handle {handle}");

        _logger.LogDebug("sp_execute: handle={Handle} SQL: {Sql}",
            handle, prepared.Sql.Length > 200 ? prepared.Sql[..200] + "..." : prepared.Sql);

        using var cmd = new SqlCommand(prepared.Sql, conn);
        cmd.CommandTimeout = 120;

        BindExecuteParameters(cmd, rpc.Parameters);

        return await _responseBuilder.BuildRpcResponseAsync(cmd, returnStatus: 0, ct: ct);
    }

    private async Task<byte[]> HandleSpPrepExecAsync(ParsedRpc rpc, SqlConnection conn, CancellationToken ct)
    {
        if (rpc.Parameters.Count < 3)
            return BuildRpcError("sp_prepexec requires handle, paramdef, and sql parameters");

        string? paramDef = rpc.Parameters[1].StringValue;
        string sql = rpc.Parameters[2].StringValue
            ?? throw new InvalidOperationException("sp_prepexec: sql parameter is null");

        int handle = _nextHandle++;
        _preparedStatements[handle] = new PreparedStatement(sql, paramDef);

        _logger.LogDebug("sp_prepexec: allocated handle {Handle} for SQL: {Sql}",
            handle, sql.Length > 200 ? sql[..200] + "..." : sql);

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 120;

        BindPrepExecParameters(cmd, rpc.Parameters);

        return await _responseBuilder.BuildRpcResponseAsync(cmd, returnStatus: 0, outputHandle: handle, ct: ct);
    }

    private byte[] HandleSpUnprepare(ParsedRpc rpc)
    {
        if (rpc.Parameters.Count < 1)
            return BuildRpcError("sp_unprepare requires a handle parameter");

        int handle = rpc.Parameters[0].IntValue ?? 0;
        bool removed = _preparedStatements.Remove(handle);
        _logger.LogDebug("sp_unprepare: handle={Handle} removed={Removed}", handle, removed);

        return BuildReturnStatusOnly(0);
    }

    private async Task<byte[]> HandleGenericRpcAsync(ParsedRpc rpc, SqlConnection conn, CancellationToken ct)
    {
        string procName = rpc.ProcId.HasValue ? $"sp_proc_{rpc.ProcId}" : rpc.ProcName!;
        _logger.LogWarning("Unsupported well-known RPC ProcId={ProcId}, executing as stored procedure '{Name}'",
            rpc.ProcId, procName);

        return await HandleNamedProcAsync(rpc with { ProcName = procName }, conn, ct);
    }

    private async Task<byte[]> HandleNamedProcAsync(ParsedRpc rpc, SqlConnection conn, CancellationToken ct)
    {
        string procName = rpc.ProcName!;
        _logger.LogDebug("Executing named stored procedure: {Name}", procName);

        using var cmd = new SqlCommand(procName, conn);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandTimeout = 120;

        var retValParam = new SqlParameter
        {
            ParameterName = "@__ReturnValue",
            SqlDbType = SqlDbType.Int,
            Direction = ParameterDirection.ReturnValue,
        };
        cmd.Parameters.Add(retValParam);

        for (int i = 0; i < rpc.Parameters.Count; i++)
        {
            var p = rpc.Parameters[i];
            var sqlParam = new SqlParameter
            {
                ParameterName = string.IsNullOrEmpty(p.Name) ? $"@p{i}" : p.Name,
                Value = p.Value ?? (object)DBNull.Value,
                Direction = p.IsOutput ? ParameterDirection.InputOutput : ParameterDirection.Input,
            };
            if (p.IsTvp)
            {
                sqlParam.SqlDbType = SqlDbType.Structured;
                sqlParam.TypeName = p.TvpTypeName;
                sqlParam.Direction = ParameterDirection.Input;
            }
            cmd.Parameters.Add(sqlParam);
        }

        return await _responseBuilder.BuildRpcResponseAsync(cmd, returnStatus: 0, ct: ct);
    }

    private static void BindUserParameters(SqlCommand cmd, List<RpcParameter> parameters)
    {
        // sp_executesql: param[0] = SQL, param[1] = param definitions, param[2..] = actual values
        if (parameters.Count <= 2)
            return;

        for (int i = 2; i < parameters.Count; i++)
        {
            var p = parameters[i];
            var sqlParam = new SqlParameter
            {
                ParameterName = string.IsNullOrEmpty(p.Name) ? $"@p{i - 2}" : p.Name,
                Value = p.Value ?? (object)DBNull.Value,
            };
            cmd.Parameters.Add(sqlParam);
        }
    }

    private static void BindExecuteParameters(SqlCommand cmd, List<RpcParameter> parameters)
    {
        // sp_execute: param[0] = handle, param[1..] = actual values
        for (int i = 1; i < parameters.Count; i++)
        {
            var p = parameters[i];
            var sqlParam = new SqlParameter
            {
                ParameterName = string.IsNullOrEmpty(p.Name) ? $"@p{i - 1}" : p.Name,
                Value = p.Value ?? (object)DBNull.Value,
            };
            cmd.Parameters.Add(sqlParam);
        }
    }

    private static void BindPrepExecParameters(SqlCommand cmd, List<RpcParameter> parameters)
    {
        // sp_prepexec: param[0] = handle OUTPUT, param[1] = paramdef, param[2] = SQL, param[3..] = values
        for (int i = 3; i < parameters.Count; i++)
        {
            var p = parameters[i];
            var sqlParam = new SqlParameter
            {
                ParameterName = string.IsNullOrEmpty(p.Name) ? $"@p{i - 3}" : p.Name,
                Value = p.Value ?? (object)DBNull.Value,
            };
            cmd.Parameters.Add(sqlParam);
        }
    }

    private byte[] BuildRpcError(string message)
    {
        _logger.LogWarning("RPC error: {Message}", message);
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        WriteReturnStatus(bw, 1);
        LoginHandler.WriteErrorToken(bw, 50000, 16, 1, message, "DbProxy", "", 0);
        WriteDoneProcToken(bw, (ushort)(TdsConstants.DoneError | TdsConstants.DoneFinal), 0);

        return ms.ToArray();
    }

    private static byte[] BuildPrepareResponse(int handle)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        WriteReturnStatus(bw, 0);
        WriteReturnValueInt(bw, "@handle", handle);
        WriteDoneProcToken(bw, TdsConstants.DoneFinal, 0);

        return ms.ToArray();
    }

    private static byte[] BuildReturnStatusOnly(int status)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        WriteReturnStatus(bw, status);
        WriteDoneProcToken(bw, TdsConstants.DoneFinal, 0);

        return ms.ToArray();
    }

    internal static void WriteReturnStatus(BinaryWriter bw, int status)
    {
        bw.Write(TdsConstants.TokenReturnStatus);
        bw.Write(status);
    }

    internal static void WriteDoneProcToken(BinaryWriter bw, ushort status, long rowCount)
    {
        bw.Write(TdsConstants.TokenDoneProc);
        bw.Write(status);
        bw.Write((ushort)0); // CurCmd
        bw.Write(rowCount);
    }

    internal static void WriteDoneInProcToken(BinaryWriter bw, ushort status, long rowCount)
    {
        bw.Write(TdsConstants.TokenDoneInProc);
        bw.Write(status);
        bw.Write((ushort)0); // CurCmd
        bw.Write(rowCount);
    }

    internal static void WriteReturnValueInt(BinaryWriter bw, string paramName, int value)
    {
        WriteReturnValue(bw, paramName, 0, value, typeof(int));
    }

    internal static void WriteReturnValue(BinaryWriter bw, string paramName, ushort ordinal, object? value, Type clrType)
    {
        bw.Write(TdsConstants.TokenReturnValue);

        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        tw.Write(ordinal);
        tw.Write((byte)paramName.Length);
        tw.Write(Encoding.Unicode.GetBytes(paramName));
        tw.Write((byte)0x01);          // Status: output param
        tw.Write((uint)0);             // UserType
        tw.Write((ushort)0);           // Flags

        if (value is null || value is DBNull)
        {
            WriteReturnValueNull(tw, clrType);
        }
        else
        {
            WriteReturnValueTyped(tw, value);
        }

        byte[] tokenData = tokenMs.ToArray();
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    private static void WriteReturnValueNull(BinaryWriter tw, Type clrType)
    {
        if (clrType == typeof(int) || clrType == typeof(long) || clrType == typeof(short) || clrType == typeof(byte))
        {
            tw.Write(TdsConstants.TypeIntN);
            tw.Write((byte)4);
            tw.Write((byte)0); // NULL
        }
        else if (clrType == typeof(decimal))
        {
            tw.Write(TdsConstants.TypeNumericN);
            tw.Write((byte)17);
            tw.Write((byte)38);
            tw.Write((byte)0);
            tw.Write((byte)0); // NULL
        }
        else if (clrType == typeof(bool))
        {
            tw.Write(TdsConstants.TypeBitN);
            tw.Write((byte)1);
            tw.Write((byte)0); // NULL
        }
        else if (clrType == typeof(byte[]))
        {
            tw.Write(TdsConstants.TypeBigVarBin);
            tw.Write((ushort)8000);
            tw.Write(unchecked((ushort)0xFFFF)); // CHARBIN_NULL
        }
        else
        {
            tw.Write(TdsConstants.TypeNVarChar);
            tw.Write((ushort)8000);
            tw.Write(TdsConstants.DefaultCollation);
            tw.Write(unchecked((ushort)0xFFFF)); // CHARBIN_NULL
        }
    }

    private static void WriteReturnValueTyped(BinaryWriter tw, object value)
    {
        switch (value)
        {
            case int intVal:
                tw.Write(TdsConstants.TypeIntN);
                tw.Write((byte)4);
                tw.Write((byte)4);
                tw.Write(intVal);
                break;

            case long longVal:
                tw.Write(TdsConstants.TypeIntN);
                tw.Write((byte)8);
                tw.Write((byte)8);
                tw.Write(longVal);
                break;

            case short shortVal:
                tw.Write(TdsConstants.TypeIntN);
                tw.Write((byte)2);
                tw.Write((byte)2);
                tw.Write(shortVal);
                break;

            case byte byteVal:
                tw.Write(TdsConstants.TypeIntN);
                tw.Write((byte)1);
                tw.Write((byte)1);
                tw.Write(byteVal);
                break;

            case bool boolVal:
                tw.Write(TdsConstants.TypeBitN);
                tw.Write((byte)1);
                tw.Write((byte)1);
                tw.Write(boolVal ? (byte)1 : (byte)0);
                break;

            case decimal decVal:
                WriteReturnValueDecimal(tw, decVal);
                break;

            case double dblVal:
                tw.Write(TdsConstants.TypeFltN);
                tw.Write((byte)8);
                tw.Write((byte)8);
                tw.Write(dblVal);
                break;

            case float fltVal:
                tw.Write(TdsConstants.TypeFltN);
                tw.Write((byte)4);
                tw.Write((byte)4);
                tw.Write(fltVal);
                break;

            case string strVal:
                WriteReturnValueNVarChar(tw, strVal);
                break;

            case byte[] binVal:
                WriteReturnValueBinary(tw, binVal);
                break;

            case Guid guidVal:
                tw.Write(TdsConstants.TypeGuid);
                tw.Write((byte)16);
                tw.Write((byte)16);
                tw.Write(guidVal.ToByteArray());
                break;

            default:
                WriteReturnValueNVarChar(tw, value.ToString() ?? "");
                break;
        }
    }

    private static void WriteReturnValueDecimal(BinaryWriter tw, decimal value)
    {
        int[] bits = decimal.GetBits(value);
        byte scale = (byte)((bits[3] >> 16) & 0xFF);
        byte precision = 38;

        tw.Write(TdsConstants.TypeNumericN);
        tw.Write((byte)17);    // MaxLength
        tw.Write(precision);
        tw.Write(scale);

        decimal abs = Math.Abs(value);
        for (int i = 0; i < scale; i++)
            abs *= 10m;

        byte sign = value >= 0 ? (byte)1 : (byte)0;

        ulong lo = (uint)decimal.GetBits(Math.Abs(value))[0] | ((ulong)(uint)decimal.GetBits(Math.Abs(value))[1] << 32);
        ulong hi = (uint)decimal.GetBits(Math.Abs(value))[2];

        int dataLen = precision switch
        {
            <= 9 => 4,
            <= 19 => 8,
            <= 28 => 12,
            _ => 16,
        };

        tw.Write((byte)(dataLen + 1));
        tw.Write(sign);

        byte[] padded = new byte[dataLen];
        BitConverter.TryWriteBytes(padded.AsSpan(0), lo);
        if (dataLen > 8)
            BitConverter.TryWriteBytes(padded.AsSpan(8), hi);
        tw.Write(padded);
    }

    private static void WriteReturnValueNVarChar(BinaryWriter tw, string value)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(value);
        int maxLen = Math.Max(bytes.Length, 2);
        if (maxLen > 8000) maxLen = 8000;

        tw.Write(TdsConstants.TypeNVarChar);
        tw.Write((ushort)maxLen);
        tw.Write(TdsConstants.DefaultCollation);
        tw.Write((ushort)bytes.Length);
        tw.Write(bytes);
    }

    private static void WriteReturnValueBinary(BinaryWriter tw, byte[] value)
    {
        int maxLen = Math.Max(value.Length, 1);
        if (maxLen > 8000) maxLen = 8000;

        tw.Write(TdsConstants.TypeBigVarBin);
        tw.Write((ushort)maxLen);
        tw.Write((ushort)value.Length);
        tw.Write(value);
    }

    #region RPC Payload Parsing

    public ParsedRpc ParseRpcRequest(ReadOnlySpan<byte> payload)
    {
        int offset = SkipAllHeaders(payload);

        ushort nameLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        ushort? procId = null;
        string? procName = null;

        if (nameLen == 0xFFFF)
        {
            procId = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
            offset += 2;
        }
        else
        {
            int byteLen = nameLen * 2;
            procName = Encoding.Unicode.GetString(payload.Slice(offset, byteLen));
            offset += byteLen;
        }

        ushort optionFlags = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        var parameters = new List<RpcParameter>();
        while (offset < payload.Length)
        {
            // Check for batch separator (0xFF or 0x80) or NoExec (0xFE)
            byte peek = payload[offset];
            if (peek == 0xFF || peek == 0x80 || peek == 0xFE)
                break;

            var (param, newOffset) = ReadParameter(payload, offset);
            parameters.Add(param);
            offset = newOffset;
        }

        return new ParsedRpc(procId, procName, optionFlags, parameters);
    }

    private static int SkipAllHeaders(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 4)
            return 0;

        uint totalLen = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        if (totalLen < 4 || totalLen > (uint)payload.Length)
            return 0;

        return (int)totalLen;
    }

    private (RpcParameter param, int newOffset) ReadParameter(ReadOnlySpan<byte> payload, int offset)
    {
        byte nameCharLen = payload[offset++];
        string name = "";
        if (nameCharLen > 0)
        {
            int nameByteLen = nameCharLen * 2;
            name = Encoding.Unicode.GetString(payload.Slice(offset, nameByteLen));
            offset += nameByteLen;
        }

        byte statusFlags = payload[offset++];
        bool isOutput = (statusFlags & 0x01) != 0;
        bool isDefault = (statusFlags & 0x02) != 0;

        byte typeId = payload[offset];
        if (typeId == TdsConstants.TypeTable)
        {
            var (tvpValue, tvpTypeName, newOff) = ReadTvpParam(payload, offset + 1);
            return (new RpcParameter(name, isOutput, isDefault, tvpValue, null, null,
                IsTvp: true, TvpTypeName: tvpTypeName), newOff);
        }

        var (value, stringValue, intValue, newOffset) = ReadTypeInfoAndValue(payload, offset);
        return (new RpcParameter(name, isOutput, isDefault, value, stringValue, intValue), newOffset);
    }

    private (object? value, string? stringValue, int? intValue, int newOffset) ReadTypeInfoAndValue(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte typeId = payload[offset++];

        switch (typeId)
        {
            case TdsConstants.TypeIntN:
                return ReadIntNParam(payload, offset);

            case TdsConstants.TypeBitN:
                return ReadBitNParam(payload, offset);

            case TdsConstants.TypeFltN:
                return ReadFltNParam(payload, offset);

            case TdsConstants.TypeMoneyN:
                return ReadFixedLenParam(payload, offset);

            case TdsConstants.TypeDateTimeN:
                return ReadFixedLenParam(payload, offset);

            case TdsConstants.TypeGuid:
                return ReadFixedLenParam(payload, offset);

            case TdsConstants.TypeNumericN: // also covers TypeDecimalN (same wire type 0x6C)
                return ReadNumericParam(payload, offset);

            case TdsConstants.TypeNVarChar:
                return ReadNVarCharParam(payload, offset);

            case TdsConstants.TypeBigVarChar:
                return ReadVarCharParam(payload, offset);

            case TdsConstants.TypeBigVarBin:
                return ReadVarBinParam(payload, offset);

            case TdsConstants.TypeDateN:
                return ReadDateParam(payload, offset);

            case TdsConstants.TypeTimeN:
            case TdsConstants.TypeDateTime2N:
            case TdsConstants.TypeDateTimeOffsetN:
                return ReadScaledTimeParam(payload, offset, typeId);

            case TdsConstants.TypeBigBinary:
                return ReadVarBinParam(payload, offset);

            default:
                _logger.LogWarning("Unsupported RPC parameter type 0x{Type:X2}, attempting fixed-len skip", typeId);
                return ReadFixedLenParam(payload, offset);
        }
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadIntNParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte maxLen = payload[offset++];
        byte actualLen = payload[offset++];

        if (actualLen == 0)
            return (null, null, null, offset);

        int? intVal = actualLen switch
        {
            1 => payload[offset],
            2 => BinaryPrimitives.ReadInt16LittleEndian(payload[offset..]),
            4 => BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
            8 => (int)BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]),
            _ => BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
        };

        object boxed = actualLen switch
        {
            1 => payload[offset],
            2 => BinaryPrimitives.ReadInt16LittleEndian(payload[offset..]),
            4 => BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
            8 => BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]),
            _ => BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
        };

        return (boxed, null, intVal, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadBitNParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte maxLen = payload[offset++];
        byte actualLen = payload[offset++];
        if (actualLen == 0)
            return (null, null, null, offset);

        bool val = payload[offset] != 0;
        return (val, null, val ? 1 : 0, offset + 1);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadFltNParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte maxLen = payload[offset++];
        byte actualLen = payload[offset++];
        if (actualLen == 0)
            return (null, null, null, offset);

        if (actualLen == 4)
        {
            float val = BitConverter.ToSingle(payload.Slice(offset, 4));
            return (val, null, null, offset + 4);
        }

        double dVal = BitConverter.ToDouble(payload.Slice(offset, 8));
        return (dVal, null, null, offset + 8);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadFixedLenParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte maxLen = payload[offset++];
        byte actualLen = payload[offset++];
        if (actualLen == 0)
            return (null, null, null, offset);

        byte[] data = payload.Slice(offset, actualLen).ToArray();
        return (data, null, null, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadNumericParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte maxLen = payload[offset++];
        byte precision = payload[offset++];
        byte scale = payload[offset++];
        byte actualLen = payload[offset++];

        if (actualLen == 0)
            return (null, null, null, offset);

        byte sign = payload[offset];
        byte[] intBytes = payload.Slice(offset + 1, actualLen - 1).ToArray();
        offset += actualLen;

        // Reconstruct the decimal from little-endian magnitude bytes
        ulong lo = 0, hi = 0;
        for (int i = Math.Min(intBytes.Length, 8) - 1; i >= 0; i--)
            lo = (lo << 8) | intBytes[i];
        for (int i = Math.Min(intBytes.Length, 16) - 1; i >= 8; i--)
            hi = (hi << 8) | intBytes[i];

        decimal result = (decimal)lo;
        if (hi != 0)
            result += (decimal)hi * (1UL << 32) * (1UL << 32);
        for (int i = 0; i < scale; i++)
            result /= 10m;
        if (sign == 0)
            result = -result;

        return (result, null, null, offset);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadNVarCharParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        ushort maxLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;
        offset += 5; // collation

        if (maxLen == 0xFFFF)
        {
            // PLP (Partially Length-Prefixed) for NVARCHAR(MAX)
            return ReadPlpNVarChar(payload, offset);
        }

        ushort actualLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        if (actualLen == 0xFFFF)
            return (null, null, null, offset);

        string val = Encoding.Unicode.GetString(payload.Slice(offset, actualLen));
        return (val, val, null, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadPlpNVarChar(
        ReadOnlySpan<byte> payload, int offset)
    {
        ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;

        if (totalLen == 0xFFFFFFFFFFFFFFFF)
            return (null, null, null, offset);

        var sb = new StringBuilder();

        while (offset < payload.Length)
        {
            uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;

            if (chunkLen == 0)
                break;

            sb.Append(Encoding.Unicode.GetString(payload.Slice(offset, (int)chunkLen)));
            offset += (int)chunkLen;
        }

        string val = sb.ToString();
        return (val, val, null, offset);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadVarCharParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        ushort maxLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;
        offset += 5; // collation

        if (maxLen == 0xFFFF)
        {
            return ReadPlpVarChar(payload, offset);
        }

        ushort actualLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        if (actualLen == 0xFFFF)
            return (null, null, null, offset);

        string val = Encoding.UTF8.GetString(payload.Slice(offset, actualLen));
        return (val, val, null, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadPlpVarChar(
        ReadOnlySpan<byte> payload, int offset)
    {
        ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;

        if (totalLen == 0xFFFFFFFFFFFFFFFF)
            return (null, null, null, offset);

        var sb = new StringBuilder();

        while (offset < payload.Length)
        {
            uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;

            if (chunkLen == 0)
                break;

            sb.Append(Encoding.UTF8.GetString(payload.Slice(offset, (int)chunkLen)));
            offset += (int)chunkLen;
        }

        string val = sb.ToString();
        return (val, val, null, offset);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadVarBinParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        ushort maxLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        if (maxLen == 0xFFFF)
        {
            return ReadPlpBinary(payload, offset);
        }

        ushort actualLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        if (actualLen == 0xFFFF)
            return (null, null, null, offset);

        byte[] val = payload.Slice(offset, actualLen).ToArray();
        return (val, null, null, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadPlpBinary(
        ReadOnlySpan<byte> payload, int offset)
    {
        ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;

        if (totalLen == 0xFFFFFFFFFFFFFFFF)
            return (null, null, null, offset);

        using var ms = new MemoryStream();
        while (offset < payload.Length)
        {
            uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;

            if (chunkLen == 0)
                break;

            ms.Write(payload.Slice(offset, (int)chunkLen));
            offset += (int)chunkLen;
        }

        byte[] val = ms.ToArray();
        return (val, null, null, offset);
    }

    private (DataTable? value, string? tvpTypeName, int newOffset) ReadTvpParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        string dbName = ReadBVarCharUtf16(payload, ref offset);
        string schemaName = ReadBVarCharUtf16(payload, ref offset);
        string typeName = ReadBVarCharUtf16(payload, ref offset);

        string fullTypeName = string.IsNullOrEmpty(schemaName)
            ? typeName
            : $"{schemaName}.{typeName}";
        if (!string.IsNullOrEmpty(dbName))
            fullTypeName = $"{dbName}.{fullTypeName}";

        ushort colCount = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
        offset += 2;

        if (colCount == 0)
        {
            _logger.LogDebug("TVP param: null TVP (colCount=0), type={TypeName}", fullTypeName);
            return (null, fullTypeName, offset);
        }

        var colMetas = new TvpColumnMeta[colCount];
        for (int c = 0; c < colCount; c++)
        {
            uint userType = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;
            ushort flags = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
            offset += 2;

            byte colTypeId = payload[offset++];
            byte maxLen = 0;
            ushort maxLen16 = 0;
            byte precision = 0;
            byte scale = 0;
            byte[]? collation = null;

            switch (colTypeId)
            {
                case TdsConstants.TypeIntN:
                case TdsConstants.TypeBitN:
                case TdsConstants.TypeFltN:
                case TdsConstants.TypeMoneyN:
                case TdsConstants.TypeDateTimeN:
                case TdsConstants.TypeGuid:
                    maxLen = payload[offset++];
                    break;

                case TdsConstants.TypeNumericN:
                    maxLen = payload[offset++];
                    precision = payload[offset++];
                    scale = payload[offset++];
                    break;

                case TdsConstants.TypeNVarChar:
                    maxLen16 = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                    offset += 2;
                    collation = payload.Slice(offset, 5).ToArray();
                    offset += 5;
                    break;

                case TdsConstants.TypeBigVarChar:
                    maxLen16 = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                    offset += 2;
                    collation = payload.Slice(offset, 5).ToArray();
                    offset += 5;
                    break;

                case TdsConstants.TypeBigVarBin:
                case TdsConstants.TypeBigBinary:
                    maxLen16 = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                    offset += 2;
                    break;

                case TdsConstants.TypeDateN:
                    break;

                case TdsConstants.TypeTimeN:
                case TdsConstants.TypeDateTime2N:
                case TdsConstants.TypeDateTimeOffsetN:
                    scale = payload[offset++];
                    break;

                default:
                    _logger.LogWarning("TVP column type 0x{Type:X2} not specifically handled, attempting fixed-len", colTypeId);
                    maxLen = payload[offset++];
                    break;
            }

            colMetas[c] = new TvpColumnMeta(colTypeId, maxLen, maxLen16, precision, scale, collation);
        }

        SkipTvpConstraints(payload, ref offset, colCount);

        var dt = BuildDataTableFromMeta(colMetas);

        while (offset < payload.Length && payload[offset] == TdsConstants.TvpRowToken)
        {
            offset++;
            var row = new object[colCount];

            for (int c = 0; c < colCount; c++)
            {
                var meta = colMetas[c];
                row[c] = ReadTvpColumnValue(payload, ref offset, meta);
            }

            dt.Rows.Add(row);
        }

        if (offset < payload.Length && payload[offset] == TdsConstants.TvpEndToken)
            offset++;

        _logger.LogDebug("TVP param: type={TypeName}, cols={Cols}, rows={Rows}",
            fullTypeName, colCount, dt.Rows.Count);

        return (dt, fullTypeName, offset);
    }

    private static string ReadBVarCharUtf16(ReadOnlySpan<byte> payload, ref int offset)
    {
        byte charLen = payload[offset++];
        if (charLen == 0)
            return "";
        int byteLen = charLen * 2;
        string val = Encoding.Unicode.GetString(payload.Slice(offset, byteLen));
        offset += byteLen;
        return val;
    }

    private static void SkipTvpConstraints(ReadOnlySpan<byte> payload, ref int offset, int colCount)
    {
        while (offset < payload.Length)
        {
            byte token = payload[offset];
            if (token == TdsConstants.TvpRowToken || token == TdsConstants.TvpEndToken)
                break;

            if (token == 0x10) // TVP_ORDER_UNIQUE
            {
                offset++;
                ushort numCols = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                offset += 2;
                for (int i = 0; i < numCols; i++)
                {
                    offset += 2; // ColNum (USHORT)
                    offset += 1; // Flags (BYTE)
                }
            }
            else if (token == 0x11) // TVP_COLUMN_FLAGS
            {
                offset++;
                int flagBytes = (colCount + 7) / 8;
                offset += flagBytes;
            }
            else
            {
                break;
            }
        }
    }

    private static DataTable BuildDataTableFromMeta(TvpColumnMeta[] metas)
    {
        var dt = new DataTable();
        for (int c = 0; c < metas.Length; c++)
        {
            Type clrType = metas[c].TypeId switch
            {
                TdsConstants.TypeIntN => metas[c].MaxLen switch
                {
                    1 => typeof(byte),
                    2 => typeof(short),
                    8 => typeof(long),
                    _ => typeof(int),
                },
                TdsConstants.TypeBitN => typeof(bool),
                TdsConstants.TypeFltN => metas[c].MaxLen <= 4 ? typeof(float) : typeof(double),
                TdsConstants.TypeNumericN => typeof(decimal),
                TdsConstants.TypeNVarChar => typeof(string),
                TdsConstants.TypeBigVarChar => typeof(string),
                TdsConstants.TypeBigVarBin or TdsConstants.TypeBigBinary => typeof(byte[]),
                TdsConstants.TypeGuid => typeof(Guid),
                TdsConstants.TypeDateN or TdsConstants.TypeDateTimeN => typeof(DateTime),
                TdsConstants.TypeTimeN => typeof(TimeSpan),
                TdsConstants.TypeDateTime2N => typeof(DateTime),
                TdsConstants.TypeDateTimeOffsetN => typeof(DateTimeOffset),
                TdsConstants.TypeMoneyN => typeof(decimal),
                _ => typeof(object),
            };

            dt.Columns.Add($"Col{c}", clrType);
        }
        return dt;
    }

    private static object ReadTvpColumnValue(ReadOnlySpan<byte> payload, ref int offset, TvpColumnMeta meta)
    {
        switch (meta.TypeId)
        {
            case TdsConstants.TypeIntN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                object val = len switch
                {
                    1 => (object)payload[offset],
                    2 => (object)BinaryPrimitives.ReadInt16LittleEndian(payload[offset..]),
                    4 => (object)BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
                    8 => (object)BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]),
                    _ => (object)BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]),
                };
                offset += len;
                return val;
            }

            case TdsConstants.TypeBitN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                bool val = payload[offset++] != 0;
                return val;
            }

            case TdsConstants.TypeFltN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                if (len == 4)
                {
                    float f = BitConverter.ToSingle(payload.Slice(offset, 4));
                    offset += 4;
                    return f;
                }
                double d = BitConverter.ToDouble(payload.Slice(offset, 8));
                offset += 8;
                return d;
            }

            case TdsConstants.TypeMoneyN:
            case TdsConstants.TypeDateTimeN:
            case TdsConstants.TypeGuid:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                byte[] data = payload.Slice(offset, len).ToArray();
                offset += len;
                if (meta.TypeId == TdsConstants.TypeGuid && len == 16)
                    return new Guid(data);
                return data;
            }

            case TdsConstants.TypeNumericN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                byte sign = payload[offset];
                byte[] intBytes = payload.Slice(offset + 1, len - 1).ToArray();
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

            case TdsConstants.TypeNVarChar:
            {
                if (meta.MaxLen16 == 0xFFFF)
                {
                    return ReadTvpPlpString(payload, ref offset, Encoding.Unicode);
                }
                ushort len = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                offset += 2;
                if (len == 0xFFFF) return DBNull.Value;
                string s = Encoding.Unicode.GetString(payload.Slice(offset, len));
                offset += len;
                return s;
            }

            case TdsConstants.TypeBigVarChar:
            {
                if (meta.MaxLen16 == 0xFFFF)
                {
                    return ReadTvpPlpString(payload, ref offset, Encoding.UTF8);
                }
                ushort len = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                offset += 2;
                if (len == 0xFFFF) return DBNull.Value;
                string s = Encoding.UTF8.GetString(payload.Slice(offset, len));
                offset += len;
                return s;
            }

            case TdsConstants.TypeBigVarBin:
            case TdsConstants.TypeBigBinary:
            {
                if (meta.MaxLen16 == 0xFFFF)
                {
                    return ReadTvpPlpBinary(payload, ref offset);
                }
                ushort len = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
                offset += 2;
                if (len == 0xFFFF) return DBNull.Value;
                byte[] data = payload.Slice(offset, len).ToArray();
                offset += len;
                return data;
            }

            case TdsConstants.TypeDateN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                byte[] data = payload.Slice(offset, len).ToArray();
                offset += len;
                return data;
            }

            case TdsConstants.TypeTimeN:
            case TdsConstants.TypeDateTime2N:
            case TdsConstants.TypeDateTimeOffsetN:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                byte[] data = payload.Slice(offset, len).ToArray();
                offset += len;
                return data;
            }

            default:
            {
                byte len = payload[offset++];
                if (len == 0) return DBNull.Value;
                byte[] data = payload.Slice(offset, len).ToArray();
                offset += len;
                return data;
            }
        }
    }

    private static object ReadTvpPlpString(ReadOnlySpan<byte> payload, ref int offset, Encoding encoding)
    {
        ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;
        if (totalLen == 0xFFFFFFFFFFFFFFFF)
            return DBNull.Value;

        var sb = new StringBuilder();
        while (offset < payload.Length)
        {
            uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;
            if (chunkLen == 0) break;
            sb.Append(encoding.GetString(payload.Slice(offset, (int)chunkLen)));
            offset += (int)chunkLen;
        }
        return sb.ToString();
    }

    private static object ReadTvpPlpBinary(ReadOnlySpan<byte> payload, ref int offset)
    {
        ulong totalLen = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;
        if (totalLen == 0xFFFFFFFFFFFFFFFF)
            return DBNull.Value;

        using var ms = new MemoryStream();
        while (offset < payload.Length)
        {
            uint chunkLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;
            if (chunkLen == 0) break;
            ms.Write(payload.Slice(offset, (int)chunkLen));
            offset += (int)chunkLen;
        }
        return ms.ToArray();
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadDateParam(
        ReadOnlySpan<byte> payload, int offset)
    {
        byte actualLen = payload[offset++];
        if (actualLen == 0)
            return (null, null, null, offset);

        byte[] data = payload.Slice(offset, actualLen).ToArray();
        return (data, null, null, offset + actualLen);
    }

    private static (object? value, string? stringValue, int? intValue, int newOffset) ReadScaledTimeParam(
        ReadOnlySpan<byte> payload, int offset, byte typeId)
    {
        byte scale = payload[offset++];
        byte actualLen = payload[offset++];

        if (actualLen == 0)
            return (null, null, null, offset);

        byte[] data = payload.Slice(offset, actualLen).ToArray();
        return (data, null, null, offset + actualLen);
    }

    #endregion
}

public sealed record ParsedRpc(
    ushort? ProcId,
    string? ProcName,
    ushort OptionFlags,
    List<RpcParameter> Parameters);

public sealed record RpcParameter(
    string Name,
    bool IsOutput,
    bool IsDefault,
    object? Value,
    string? StringValue,
    int? IntValue,
    bool IsTvp = false,
    string? TvpTypeName = null);

public sealed record PreparedStatement(string Sql, string? ParamDefinitions);

internal sealed record TvpColumnMeta(
    byte TypeId,
    byte MaxLen,
    ushort MaxLen16,
    byte Precision,
    byte Scale,
    byte[]? Collation);
