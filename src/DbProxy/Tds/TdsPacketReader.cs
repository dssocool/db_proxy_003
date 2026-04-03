using System.Net.Sockets;

namespace DbProxy.Tds;

/// <summary>
/// Reads complete TDS messages (potentially spanning multiple packets) from a NetworkStream.
/// Returns the reassembled payload with the packet type from the first packet.
/// </summary>
public sealed class TdsPacketReader
{
    private readonly NetworkStream _stream;
    private readonly byte[] _headerBuf = new byte[TdsConstants.PacketHeaderSize];

    public TdsPacketReader(NetworkStream stream)
    {
        _stream = stream;
    }

    public async Task<(byte packetType, byte[] payload)?> ReadMessageAsync(CancellationToken ct = default)
    {
        using var ms = new MemoryStream();
        byte packetType = 0;
        bool first = true;

        while (true)
        {
            int read = await ReadExactAsync(_stream, _headerBuf, TdsConstants.PacketHeaderSize, ct);
            if (read == 0)
                return null;

            var header = TdsPacketHeader.Read(_headerBuf);
            if (first)
            {
                packetType = header.Type;
                first = false;
            }

            int payloadLen = header.Length - TdsConstants.PacketHeaderSize;
            if (payloadLen > 0)
            {
                var payloadBuf = new byte[payloadLen];
                int payloadRead = await ReadExactAsync(_stream, payloadBuf, payloadLen, ct);
                if (payloadRead == 0)
                    return null;
                ms.Write(payloadBuf, 0, payloadLen);
            }

            if (header.IsEndOfMessage)
                break;
        }

        return (packetType, ms.ToArray());
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
}
