namespace DbProxy.Tds;

public static class TdsConstants
{
    public const int PacketHeaderSize = 8;
    public const int DefaultPacketSize = 4096;
    public const int MaxPacketSize = 32767;

    // LOGIN7 (client->server) uses little-endian: 04 00 00 74
    public static readonly byte[] TdsVersion74LoginRequest = [0x04, 0x00, 0x00, 0x74];
    // LOGINACK (server->client) uses big-endian: 74 00 00 04
    public static readonly byte[] TdsVersion74LoginAck = [0x74, 0x00, 0x00, 0x04];

    // Packet types
    public const byte PacketTypeSqlBatch = 0x01;
    public const byte PacketTypeRpcRequest = 0x03;
    public const byte PacketTypeTabularResult = 0x04;
    public const byte PacketTypeAttention = 0x06;
    public const byte PacketTypeLogin7 = 0x10;
    public const byte PacketTypePreLogin = 0x12;

    // Packet status flags
    public const byte StatusNormal = 0x00;
    public const byte StatusEndOfMessage = 0x01;

    // PRELOGIN option tokens
    public const byte PreLoginVersion = 0x00;
    public const byte PreLoginEncryption = 0x01;
    public const byte PreLoginInstOpt = 0x02;
    public const byte PreLoginThreadId = 0x03;
    public const byte PreLoginMars = 0x04;
    public const byte PreLoginTraceId = 0x05;
    public const byte PreLoginFedAuthRequired = 0x06;
    public const byte PreLoginTerminator = 0xFF;

    // PRELOGIN encryption values
    public const byte EncryptOff = 0x00;
    public const byte EncryptOn = 0x01;
    public const byte EncryptNotSup = 0x02;
    public const byte EncryptRequired = 0x03;

    // Token types (server -> client)
    public const byte TokenReturnStatus = 0x79;
    public const byte TokenReturnValue = 0xAC;
    public const byte TokenError = 0xAA;
    public const byte TokenLoginAck = 0xAD;
    public const byte TokenEnvChange = 0xE3;
    public const byte TokenDone = 0xFD;
    public const byte TokenDoneProc = 0xFE;
    public const byte TokenDoneInProc = 0xFF;
    public const byte TokenColMetadata = 0x81;
    public const byte TokenRow = 0xD1;
    public const byte TokenInfo = 0xAB;
    public const byte TokenOrder = 0xA9;

    // Well-known RPC ProcIDs (used when NameLenProcID starts with 0xFFFF)
    public const ushort SpExecuteSql = 10;
    public const ushort SpPrepare = 11;
    public const ushort SpExecute = 12;
    public const ushort SpPrepExec = 13;
    public const ushort SpUnprepare = 15;

    // ENVCHANGE types
    public const byte EnvDatabase = 1;
    public const byte EnvLanguage = 2;
    public const byte EnvCharset = 3;
    public const byte EnvPacketSize = 4;
    public const byte EnvCollation = 7;

    // DONE status flags
    public const ushort DoneFinal = 0x0000;
    public const ushort DoneMore = 0x0001;
    public const ushort DoneError = 0x0002;
    public const ushort DoneCount = 0x0010;

    // TDS type IDs for COLMETADATA
    public const byte TypeIntN = 0x26;
    public const byte TypeBitN = 0x68;
    public const byte TypeFltN = 0x6D;
    public const byte TypeDateTimeN = 0x6F;
    public const byte TypeNumericN = 0x6C;
    public const byte TypeDecimalN = 0x6C; // same wire type as NumericN
    public const byte TypeGuid = 0x24;
    public const byte TypeNVarChar = 0xE7;
    public const byte TypeBigVarChar = 0xA7;
    public const byte TypeBigVarBin = 0xA5;
    public const byte TypeBigBinary = 0xAD;
    public const byte TypeMoneyN = 0x6E;
    public const byte TypeDateN = 0x28;
    public const byte TypeTimeN = 0x29;
    public const byte TypeDateTime2N = 0x2A;
    public const byte TypeDateTimeOffsetN = 0x2B;
    public const byte TypeXml = 0xF1;
    public const byte TypeNText = 0x63;
    public const byte TypeText = 0x23;
    public const byte TypeImage = 0x22;
    public const byte TypeTable = 0xF3;

    public const byte TvpRowToken = 0x01;
    public const byte TvpEndToken = 0x00;

    // Collation for NVARCHAR (raw_collation for Windows Latin1)
    public static readonly byte[] DefaultCollation = [0x09, 0x04, 0xD0, 0x00, 0x34];

    // LOGIN7 fixed header size
    public const int Login7FixedHeaderSize = 94;
}
