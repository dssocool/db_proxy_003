using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace DbProxy.Tds;

/// <summary>
/// Reads complete TDS messages (potentially spanning multiple packets) from a NetworkStream.
/// Returns the reassembled payload with the packet type from the first packet.
/// </summary>
public sealed class TdsPacketReader
{
    private readonly NetworkStream _stream;
    private readonly ILogger? _logger;
    private readonly byte[] _headerBuf = new byte[TdsConstants.PacketHeaderSize];

    public TdsPacketReader(NetworkStream stream, ILogger? logger = null)
    {
        _stream = stream;
        _logger = logger;
    }

    public async Task<(byte packetType, byte[] payload)?> ReadMessageAsync(CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        byte packetType = 0;
        bool first = true;
        int packetCount = 0;

        _logger?.LogDebug("Waiting for TDS packet from client...");

        while (true)
        {
            int read = await ReadExactAsync(_stream, _headerBuf, TdsConstants.PacketHeaderSize, ct);
            if (read == 0)
            {
                _logger?.LogDebug("ReadExact returned 0 bytes (client closed connection)");
                return null;
            }

            var header = TdsPacketHeader.Read(_headerBuf);
            packetCount++;

            if (first)
            {
                packetType = header.Type;
                first = false;
            }

            _logger?.LogDebug(
                "Packet #{Num}: Type=0x{Type:X2}({TypeName}) Status=0x{Status:X2} Length={Len} SPID={Spid} PacketId={PktId} EOM={Eom}",
                packetCount, header.Type, TdsPacketTypeName(header.Type),
                header.Status, header.Length, header.Spid, header.PacketId, header.IsEndOfMessage);

            _logger?.LogDebug("  Header hex: {Hex}", BitConverter.ToString(_headerBuf, 0, TdsConstants.PacketHeaderSize));

            int payloadLen = header.Length - TdsConstants.PacketHeaderSize;
            if (payloadLen > 0)
            {
                var payloadBuf = new byte[payloadLen];
                int payloadRead = await ReadExactAsync(_stream, payloadBuf, payloadLen, ct);
                if (payloadRead == 0)
                {
                    _logger?.LogDebug("ReadExact returned 0 bytes while reading payload ({PayloadLen} bytes expected)", payloadLen);
                    return null;
                }
                ms.Write(payloadBuf, 0, payloadLen);

                if (_logger?.IsEnabled(LogLevel.Debug) == true)
                {
                    int dumpLen = Math.Min(payloadLen, 128);
                    _logger.LogDebug("  Payload ({PayloadLen} bytes, showing first {DumpLen}): {Hex}",
                        payloadLen, dumpLen, BitConverter.ToString(payloadBuf, 0, dumpLen));
                }
            }
            else
            {
                _logger?.LogDebug("  Packet has no payload (header-only)");
            }

            if (header.IsEndOfMessage)
                break;
        }

        var totalPayload = ms.ToArray();
        _logger?.LogDebug("Complete TDS message: Type=0x{Type:X2}({TypeName}) TotalPayload={Len} bytes across {Count} packet(s)",
            packetType, TdsPacketTypeName(packetType), totalPayload.Length, packetCount);

        return (packetType, totalPayload);
    }

    private static async Task<int> ReadExactAsync(NetworkStream stream, byte[] buffer, int count, CancellationToken ct)
    {
        int offset = 0;
        while (offset < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), ct);
            if (n == 0)
                return 0;
            offset += n;
        }
        return offset;
    }

    private static string TdsPacketTypeName(byte type) => type switch
    {
        TdsConstants.PacketTypeSqlBatch => "SQL_BATCH",
        TdsConstants.PacketTypeRpcRequest => "RPC_REQUEST",
        TdsConstants.PacketTypeTabularResult => "TABULAR_RESULT",
        TdsConstants.PacketTypeAttention => "ATTENTION",
        TdsConstants.PacketTypeLogin7 => "LOGIN7",
        TdsConstants.PacketTypePreLogin => "PRELOGIN",
        0x11 => "SSPI",
        0x14 => "PRELOGIN_SSL",
        _ => $"UNKNOWN(0x{type:X2})",
    };
}
