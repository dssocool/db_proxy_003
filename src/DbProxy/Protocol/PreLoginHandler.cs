using System.Buffers.Binary;
using DbProxy.Tds;
using Microsoft.Extensions.Logging;

namespace DbProxy.Protocol;

public sealed class PreLoginHandler
{
    private readonly ILogger _logger;

    public PreLoginHandler(ILogger logger)
    {
        _logger = logger;
    }

    public sealed class PreLoginResult
    {
        public byte Encryption { get; set; } = TdsConstants.EncryptNotSup;
        public bool MarsRequested { get; set; }
    }

    /// <summary>
    /// Parse the client's PRELOGIN payload to extract encryption and MARS preferences.
    /// </summary>
    public PreLoginResult ParseClientPreLogin(ReadOnlySpan<byte> payload)
    {
        var result = new PreLoginResult();
        int offset = 0;

        _logger.LogDebug("Parsing client PRELOGIN ({Len} bytes)", payload.Length);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            int dumpLen = Math.Min(payload.Length, 256);
            _logger.LogDebug("  PRELOGIN hex dump (first {DumpLen} bytes): {Hex}",
                dumpLen, BitConverter.ToString(payload[..dumpLen].ToArray()));
        }

        while (offset < payload.Length)
        {
            byte token = payload[offset];
            if (token == TdsConstants.PreLoginTerminator)
            {
                _logger.LogDebug("  Option: TERMINATOR at offset {Offset}", offset);
                break;
            }

            if (offset + 4 >= payload.Length)
            {
                _logger.LogWarning("  Truncated option header at offset {Offset}", offset);
                break;
            }

            ushort dataOffset = BinaryPrimitives.ReadUInt16BigEndian(payload[(offset + 1)..]);
            ushort dataLength = BinaryPrimitives.ReadUInt16BigEndian(payload[(offset + 3)..]);

            string tokenName = PreLoginTokenName(token);
            _logger.LogDebug("  Option: {Token}(0x{TokenHex:X2}) DataOffset={DataOff} DataLength={DataLen}",
                tokenName, token, dataOffset, dataLength);

            if (dataOffset < payload.Length && dataLength > 0)
            {
                int safeLen = Math.Min(dataLength, payload.Length - dataOffset);
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("    Data: {Hex}",
                        BitConverter.ToString(payload.Slice(dataOffset, safeLen).ToArray()));
                }

                if (token == TdsConstants.PreLoginVersion && safeLen >= 6)
                {
                    byte major = payload[dataOffset];
                    byte minor = payload[dataOffset + 1];
                    ushort build = BinaryPrimitives.ReadUInt16BigEndian(payload[(dataOffset + 2)..]);
                    ushort subBuild = BinaryPrimitives.ReadUInt16BigEndian(payload[(dataOffset + 4)..]);
                    _logger.LogDebug("    Client TDS Version: {Major}.{Minor}.{Build}.{SubBuild}", major, minor, build, subBuild);
                }
            }

            if (token == TdsConstants.PreLoginEncryption && dataLength >= 1 && dataOffset < payload.Length)
            {
                result.Encryption = payload[dataOffset];
                string encName = result.Encryption switch
                {
                    TdsConstants.EncryptOff => "ENCRYPT_OFF",
                    TdsConstants.EncryptOn => "ENCRYPT_ON",
                    TdsConstants.EncryptNotSup => "ENCRYPT_NOT_SUP",
                    TdsConstants.EncryptRequired => "ENCRYPT_REQ",
                    _ => $"UNKNOWN(0x{result.Encryption:X2})",
                };
                _logger.LogDebug("    Client requested encryption: {EncName} (0x{Enc:X2})", encName, result.Encryption);

                if (result.Encryption == TdsConstants.EncryptOn || result.Encryption == TdsConstants.EncryptRequired)
                {
                    _logger.LogWarning("    Client requires encryption (0x{Enc:X2}) but proxy does not support TLS. " +
                        "The client (SSMS) may hang or fail after PRELOGIN if it expects a TLS handshake.", result.Encryption);
                }
            }

            if (token == TdsConstants.PreLoginMars && dataLength >= 1 && dataOffset < payload.Length)
            {
                result.MarsRequested = payload[dataOffset] != 0;
                _logger.LogDebug("    Client MARS requested: {Mars}", result.MarsRequested);
            }

            offset += 5;
        }

        return result;
    }

    /// <summary>
    /// Builds the server PRELOGIN response payload.
    /// Advertises VERSION, ENCRYPTION=NOT_SUP, INSTOPT, THREADID=0, MARS=0, then TERMINATOR.
    /// </summary>
    public byte[] BuildServerPreLoginResponse()
    {
        const int optionCount = 5;
        const int optionHeaderSize = optionCount * 5 + 1;

        const int versionLen = 6;
        const int encryptionLen = 1;
        const int instOptLen = 1;
        const int threadIdLen = 4;
        const int marsLen = 1;
        const int totalDataLen = versionLen + encryptionLen + instOptLen + threadIdLen + marsLen;
        const int totalLen = optionHeaderSize + totalDataLen;

        var buf = new byte[totalLen];
        int headerPos = 0;
        int dataPos = optionHeaderSize;

        void WriteOption(byte token, int len)
        {
            buf[headerPos++] = token;
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(headerPos), (ushort)dataPos);
            headerPos += 2;
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(headerPos), (ushort)len);
            headerPos += 2;
        }

        WriteOption(TdsConstants.PreLoginVersion, versionLen);
        buf[dataPos++] = 15;
        buf[dataPos++] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 2;
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 2;

        WriteOption(TdsConstants.PreLoginEncryption, encryptionLen);
        buf[dataPos++] = TdsConstants.EncryptNotSup;

        WriteOption(TdsConstants.PreLoginInstOpt, instOptLen);
        buf[dataPos++] = 0x00;

        WriteOption(TdsConstants.PreLoginThreadId, threadIdLen);
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 4;

        WriteOption(TdsConstants.PreLoginMars, marsLen);
        buf[dataPos++] = 0x01;

        buf[headerPos] = TdsConstants.PreLoginTerminator;

        _logger.LogDebug("Built server PRELOGIN response ({Len} bytes): VERSION=15.0 ENCRYPTION=NOT_SUP MARS=on", totalLen);
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("  PRELOGIN response hex: {Hex}", BitConverter.ToString(buf));
        }

        return buf;
    }

    private static string PreLoginTokenName(byte token) => token switch
    {
        TdsConstants.PreLoginVersion => "VERSION",
        TdsConstants.PreLoginEncryption => "ENCRYPTION",
        TdsConstants.PreLoginInstOpt => "INSTOPT",
        TdsConstants.PreLoginThreadId => "THREADID",
        TdsConstants.PreLoginMars => "MARS",
        TdsConstants.PreLoginTraceId => "TRACEID",
        TdsConstants.PreLoginFedAuthRequired => "FEDAUTHREQUIRED",
        TdsConstants.PreLoginTerminator => "TERMINATOR",
        _ => $"UNKNOWN(0x{token:X2})",
    };
}
