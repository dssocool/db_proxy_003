using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace DbProxy.Tds;

/// <summary>
/// Writes TDS messages to a NetworkStream, splitting into packets of at most <see cref="PacketSize"/> bytes.
/// </summary>
public sealed class TdsPacketWriter
{
    private readonly NetworkStream _stream;
    private readonly ILogger? _logger;
    private byte _packetId;

    public int PacketSize { get; set; } = TdsConstants.DefaultPacketSize;

    public TdsPacketWriter(NetworkStream stream, ILogger? logger = null)
    {
        _stream = stream;
        _logger = logger;
    }

    public async Task WriteMessageAsync(byte packetType, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        int maxPayloadPerPacket = PacketSize - TdsConstants.PacketHeaderSize;
        int offset = 0;
        int remaining = payload.Length;

        _logger?.LogDebug("Writing TDS message: Type=0x{Type:X2}({TypeName}) TotalPayload={Len} bytes PacketSize={PktSize}",
            packetType, TdsPacketTypeName(packetType), payload.Length, PacketSize);

        if (remaining == 0)
        {
            await WriteSinglePacketAsync(packetType, ReadOnlyMemory<byte>.Empty, true, ct);
            return;
        }

        int packetNum = 0;
        while (remaining > 0)
        {
            int chunk = Math.Min(remaining, maxPayloadPerPacket);
            bool isLast = (remaining - chunk) == 0;
            packetNum++;
            await WriteSinglePacketAsync(packetType, payload.Slice(offset, chunk), isLast, ct);
            _logger?.LogDebug("  Sent packet #{Num}: {Chunk} bytes payload, EOM={IsLast}", packetNum, chunk, isLast);
            offset += chunk;
            remaining -= chunk;
        }
    }

    private async Task WriteSinglePacketAsync(byte packetType, ReadOnlyMemory<byte> payload, bool isLast, CancellationToken ct)
    {
        ushort totalLen = (ushort)(TdsConstants.PacketHeaderSize + payload.Length);
        var header = new TdsPacketHeader
        {
            Type = packetType,
            Status = isLast ? TdsConstants.StatusEndOfMessage : TdsConstants.StatusNormal,
            Length = totalLen,
            Spid = 0,
            PacketId = ++_packetId,
            Window = 0,
        };

        var headerBytes = new byte[TdsConstants.PacketHeaderSize];
        header.WriteTo(headerBytes);

        _logger?.LogDebug("  Write header: {Hex}", BitConverter.ToString(headerBytes));

        if (_logger?.IsEnabled(LogLevel.Debug) == true && payload.Length > 0)
        {
            int dumpLen = Math.Min(payload.Length, 128);
            _logger.LogDebug("  Write payload ({PayloadLen} bytes, showing first {DumpLen}): {Hex}",
                payload.Length, dumpLen, BitConverter.ToString(payload.Span[..dumpLen].ToArray()));
        }

        await _stream.WriteAsync(headerBytes, ct);
        if (payload.Length > 0)
            await _stream.WriteAsync(payload, ct);
        await _stream.FlushAsync(ct);
    }

    private static string TdsPacketTypeName(byte type) => type switch
    {
        TdsConstants.PacketTypeSqlBatch => "SQL_BATCH",
        TdsConstants.PacketTypeRpcRequest => "RPC_REQUEST",
        TdsConstants.PacketTypeTabularResult => "TABULAR_RESULT",
        TdsConstants.PacketTypeAttention => "ATTENTION",
        TdsConstants.PacketTypeLogin7 => "LOGIN7",
        TdsConstants.PacketTypePreLogin => "PRELOGIN",
        _ => $"UNKNOWN(0x{type:X2})",
    };
}
