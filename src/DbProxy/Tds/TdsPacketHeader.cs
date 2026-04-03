using System.Buffers.Binary;

namespace DbProxy.Tds;

public struct TdsPacketHeader
{
    public byte Type;
    public byte Status;
    public ushort Length;
    public ushort Spid;
    public byte PacketId;
    public byte Window;

    public static TdsPacketHeader Read(ReadOnlySpan<byte> buffer)
    {
        return new TdsPacketHeader
        {
            Type = buffer[0],
            Status = buffer[1],
            Length = BinaryPrimitives.ReadUInt16BigEndian(buffer[2..]),
            Spid = BinaryPrimitives.ReadUInt16BigEndian(buffer[4..]),
            PacketId = buffer[6],
            Window = buffer[7],
        };
    }

    public readonly void WriteTo(Span<byte> buffer)
    {
        buffer[0] = Type;
        buffer[1] = Status;
        BinaryPrimitives.WriteUInt16BigEndian(buffer[2..], Length);
        BinaryPrimitives.WriteUInt16BigEndian(buffer[4..], Spid);
        buffer[6] = PacketId;
        buffer[7] = Window;
    }

    public readonly bool IsEndOfMessage => (Status & TdsConstants.StatusEndOfMessage) != 0;
}
