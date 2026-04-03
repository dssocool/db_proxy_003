using System.Buffers.Binary;
using System.Text;
using DbProxy.Config;
using DbProxy.Tds;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

public sealed class LoginHandler
{
    private readonly ILogger _logger;

    public LoginHandler(ILogger logger)
    {
        _logger = logger;
    }

    public sealed class Login7Info
    {
        public string HostName { get; set; } = "";
        public string UserName { get; set; } = "";
        public string Password { get; set; } = "";
        public string AppName { get; set; } = "";
        public string ServerName { get; set; } = "";
        public string Database { get; set; } = "";
        public uint PacketSize { get; set; }
        public uint TdsVersion { get; set; }
    }

    /// <summary>
    /// Parses a LOGIN7 payload and extracts authentication fields.
    /// LOGIN7 fixed header is 94 bytes, followed by variable-length fields
    /// referenced by offset/length pairs starting at byte offset 36.
    /// </summary>
    public Login7Info ParseLogin7(ReadOnlySpan<byte> payload)
    {
        var info = new Login7Info();

        if (payload.Length < TdsConstants.Login7FixedHeaderSize)
            throw new InvalidOperationException($"LOGIN7 payload too short: {payload.Length} bytes");

        info.TdsVersion = BinaryPrimitives.ReadUInt32LittleEndian(payload[4..]);
        info.PacketSize = BinaryPrimitives.ReadUInt32LittleEndian(payload[8..]);

        // Variable-length field offsets start at byte 36 in the LOGIN7 payload.
        // Each is a pair of (ushort ibOffset, ushort cchLength) where cchLength is in UNICODE chars.
        // Fields in order: HostName, UserName, Password, AppName, ServerName,
        //                  Unused, CltIntName, Language, Database, ClientID(6 bytes), SSPI, ...
        info.HostName = ReadUnicodeField(payload, 36);
        info.UserName = ReadUnicodeField(payload, 40);
        info.Password = ReadPasswordField(payload, 44);
        info.AppName = ReadUnicodeField(payload, 48);
        info.ServerName = ReadUnicodeField(payload, 52);

        // Skip byte 56-59 (unused/extension), 60-63 (CltIntName), 64-67 (Language)
        info.Database = ReadUnicodeField(payload, 68);

        _logger.LogInformation("LOGIN7 from User={User}, App={App}, Host={Host}, DB={Db}",
            info.UserName, info.AppName, info.HostName, info.Database);

        return info;
    }

    public bool ValidateCredentials(Login7Info info, ProxyConfig config)
    {
        return string.Equals(info.UserName, config.SqlUsername, StringComparison.OrdinalIgnoreCase)
            && string.Equals(info.Password, config.SqlPassword, StringComparison.Ordinal);
    }

    /// <summary>
    /// Builds a successful login response: LOGINACK + ENVCHANGE(database) + ENVCHANGE(packetsize) + DONE
    /// </summary>
    public byte[] BuildLoginResponse(string database, int packetSize)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);

        WriteLoginAckToken(bw);
        WriteEnvChangeDatabase(bw, database);
        WriteEnvChangePacketSize(bw, packetSize);
        WriteEnvChangeLanguage(bw);
        WriteDoneToken(bw, TdsConstants.DoneFinal, 0);

        return ms.ToArray();
    }

    /// <summary>
    /// Builds a login failure ERROR + DONE response.
    /// </summary>
    public byte[] BuildLoginErrorResponse(string message)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.Unicode, leaveOpen: true);

        WriteErrorToken(bw, 18456, 14, 1, message, "DbProxy", "", 1);
        WriteDoneToken(bw, TdsConstants.DoneError | TdsConstants.DoneFinal, 0);

        return ms.ToArray();
    }

    private void WriteLoginAckToken(BinaryWriter bw)
    {
        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        // Interface: 1 = SQL_TSQL
        tw.Write((byte)1);

        // TDS Version 7.4 as little-endian DWORD
        tw.Write(TdsConstants.TdsVersion74);

        // Server program name as B_VARCHAR (length-prefixed byte, then UTF-16LE)
        string progName = "DbProxy";
        tw.Write((byte)progName.Length);
        tw.Write(Encoding.Unicode.GetBytes(progName));

        // Server program version: major.minor.buildHi.buildLo
        tw.Write((byte)1);
        tw.Write((byte)0);
        tw.Write((byte)0);
        tw.Write((byte)0);

        byte[] tokenData = tokenMs.ToArray();

        bw.Write(TdsConstants.TokenLoginAck);
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    private static void WriteEnvChangeDatabase(BinaryWriter bw, string database)
    {
        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        tw.Write(TdsConstants.EnvDatabase);

        // New value (B_VARCHAR: length byte + UTF-16LE)
        tw.Write((byte)database.Length);
        tw.Write(Encoding.Unicode.GetBytes(database));

        // Old value (empty)
        tw.Write((byte)0);

        byte[] tokenData = tokenMs.ToArray();
        bw.Write(TdsConstants.TokenEnvChange);
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    private static void WriteEnvChangePacketSize(BinaryWriter bw, int packetSize)
    {
        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        tw.Write(TdsConstants.EnvPacketSize);

        string newVal = packetSize.ToString();
        string oldVal = TdsConstants.DefaultPacketSize.ToString();

        tw.Write((byte)newVal.Length);
        tw.Write(Encoding.Unicode.GetBytes(newVal));
        tw.Write((byte)oldVal.Length);
        tw.Write(Encoding.Unicode.GetBytes(oldVal));

        byte[] tokenData = tokenMs.ToArray();
        bw.Write(TdsConstants.TokenEnvChange);
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    private static void WriteEnvChangeLanguage(BinaryWriter bw)
    {
        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        tw.Write(TdsConstants.EnvLanguage);

        string lang = "us_english";
        tw.Write((byte)lang.Length);
        tw.Write(Encoding.Unicode.GetBytes(lang));
        tw.Write((byte)0);

        byte[] tokenData = tokenMs.ToArray();
        bw.Write(TdsConstants.TokenEnvChange);
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    internal static void WriteDoneToken(BinaryWriter bw, ushort status, long rowCount)
    {
        bw.Write(TdsConstants.TokenDone);
        bw.Write(status);
        bw.Write((ushort)0); // CurCmd
        bw.Write(rowCount);  // DoneRowCount (8 bytes)
    }

    internal static void WriteErrorToken(BinaryWriter bw, int number, byte severity, byte state,
        string message, string serverName, string procName, int lineNumber)
    {
        using var tokenMs = new MemoryStream();
        using var tw = new BinaryWriter(tokenMs, Encoding.Unicode, leaveOpen: true);

        tw.Write(number);
        tw.Write(state);
        tw.Write(severity);

        // Message text: US_VARCHAR = ushort length (chars) + UTF-16LE
        tw.Write((ushort)message.Length);
        tw.Write(Encoding.Unicode.GetBytes(message));

        // Server name
        tw.Write((byte)serverName.Length);
        tw.Write(Encoding.Unicode.GetBytes(serverName));

        // Proc name
        tw.Write((byte)procName.Length);
        if (procName.Length > 0)
            tw.Write(Encoding.Unicode.GetBytes(procName));

        // Line number (DWORD for TDS 7.2+)
        tw.Write(lineNumber);

        byte[] tokenData = tokenMs.ToArray();
        bw.Write(TdsConstants.TokenError);
        bw.Write((ushort)tokenData.Length);
        bw.Write(tokenData);
    }

    private static string ReadUnicodeField(ReadOnlySpan<byte> payload, int metaOffset)
    {
        ushort ibOffset = BinaryPrimitives.ReadUInt16LittleEndian(payload[metaOffset..]);
        ushort cchLength = BinaryPrimitives.ReadUInt16LittleEndian(payload[(metaOffset + 2)..]);
        if (cchLength == 0)
            return "";
        int byteLen = cchLength * 2;
        if (ibOffset + byteLen > payload.Length)
            return "";
        return Encoding.Unicode.GetString(payload.Slice(ibOffset, byteLen));
    }

    /// <summary>
    /// Reads and decodes a TDS LOGIN7 password field.
    /// Each byte is XORed with 0xA5, then the high and low nibbles are swapped.
    /// </summary>
    private static string ReadPasswordField(ReadOnlySpan<byte> payload, int metaOffset)
    {
        ushort ibOffset = BinaryPrimitives.ReadUInt16LittleEndian(payload[metaOffset..]);
        ushort cchLength = BinaryPrimitives.ReadUInt16LittleEndian(payload[(metaOffset + 2)..]);
        if (cchLength == 0)
            return "";
        int byteLen = cchLength * 2;
        if (ibOffset + byteLen > payload.Length)
            return "";

        byte[] decoded = new byte[byteLen];
        for (int i = 0; i < byteLen; i++)
        {
            byte b = payload[ibOffset + i];
            b ^= 0xA5;
            b = (byte)(((b & 0x0F) << 4) | ((b & 0xF0) >> 4));
            decoded[i] = b;
        }
        return Encoding.Unicode.GetString(decoded);
    }
}
