using System.Net.Sockets;

namespace DbProxy.Tds;

/// <summary>
/// Writes TDS messages to a NetworkStream, splitting into packets of at most <see cref="PacketSize"/> bytes.
/// </summary>
public sealed class TdsPacketWriter
{
    private readonly NetworkStream _stream;
    private byte _packetId;

    public int PacketSize { get; set; } = TdsConstants.DefaultPacketSize;

    public TdsPacketWriter(NetworkStream stream)
    {
        _stream = stream;
    }

    public async Task WriteMessageAsync(byte packetType, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        int maxPayloadPerPacket = PacketSize - TdsConstants.PacketHeaderSize;
        int offset = 0;
        int remaining = payload.Length;

        if (remaining == 0)
        {
            await WriteSinglePacketAsync(packetType, ReadOnlyMemory<byte>.Empty, true, ct);
            return;
        }

        while (remaining > 0)
        {
            int chunk = Math.Min(remaining, maxPayloadPerPacket);
            bool isLast = (remaining - chunk) == 0;
            await WriteSinglePacketAsync(packetType, payload.Slice(offset, chunk), isLast, ct);
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

        await _stream.WriteAsync(headerBytes, ct);
        if (payload.Length > 0)
            await _stream.WriteAsync(payload, ct);
        await _stream.FlushAsync(ct);
    }
}
