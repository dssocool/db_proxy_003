using System.Buffers.Binary;

namespace DbProxy.Tds;

public static class SmpConstants
{
    public const int HeaderSize = 16;
    public const byte SmId = 0x53;

    public const byte FlagSyn = 0x01;
    public const byte FlagAck = 0x02;
    public const byte FlagFin = 0x04;
    public const byte FlagData = 0x08;

    public const uint DefaultWindow = 4096;
}

public struct SmpHeader
{
    public byte SmId;
    public byte Flags;
    public ushort Sid;
    public uint Length;
    public uint SeqNum;
    public uint Window;

    public static SmpHeader Read(ReadOnlySpan<byte> buffer)
    {
        return new SmpHeader
        {
            SmId = buffer[0],
            Flags = buffer[1],
            Sid = BinaryPrimitives.ReadUInt16LittleEndian(buffer[2..]),
            Length = BinaryPrimitives.ReadUInt32LittleEndian(buffer[4..]),
            SeqNum = BinaryPrimitives.ReadUInt32LittleEndian(buffer[8..]),
            Window = BinaryPrimitives.ReadUInt32LittleEndian(buffer[12..]),
        };
    }

    public readonly void WriteTo(Span<byte> buffer)
    {
        buffer[0] = SmId;
        buffer[1] = Flags;
        BinaryPrimitives.WriteUInt16LittleEndian(buffer[2..], Sid);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[4..], Length);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[8..], SeqNum);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[12..], Window);
    }

    public readonly bool IsSyn => Flags == SmpConstants.FlagSyn;
    public readonly bool IsAck => Flags == SmpConstants.FlagAck;
    public readonly bool IsFin => Flags == SmpConstants.FlagFin;
    public readonly bool IsData => Flags == SmpConstants.FlagData;

    public readonly int DataLength => (int)(Length - SmpConstants.HeaderSize);

    public static byte[] BuildSyn(ushort sid, uint window)
    {
        var buf = new byte[SmpConstants.HeaderSize];
        new SmpHeader
        {
            SmId = SmpConstants.SmId,
            Flags = SmpConstants.FlagSyn,
            Sid = sid,
            Length = SmpConstants.HeaderSize,
            SeqNum = 0,
            Window = window,
        }.WriteTo(buf);
        return buf;
    }

    public static byte[] BuildAck(ushort sid, uint seqNum, uint window)
    {
        var buf = new byte[SmpConstants.HeaderSize];
        new SmpHeader
        {
            SmId = SmpConstants.SmId,
            Flags = SmpConstants.FlagAck,
            Sid = sid,
            Length = SmpConstants.HeaderSize,
            SeqNum = seqNum,
            Window = window,
        }.WriteTo(buf);
        return buf;
    }

    public static byte[] BuildFin(ushort sid, uint seqNum, uint window)
    {
        var buf = new byte[SmpConstants.HeaderSize];
        new SmpHeader
        {
            SmId = SmpConstants.SmId,
            Flags = SmpConstants.FlagFin,
            Sid = sid,
            Length = SmpConstants.HeaderSize,
            SeqNum = seqNum,
            Window = window,
        }.WriteTo(buf);
        return buf;
    }
}
