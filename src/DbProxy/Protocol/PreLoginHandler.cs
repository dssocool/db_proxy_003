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

    /// <summary>
    /// Parse the client's PRELOGIN payload to extract the requested encryption level.
    /// Returns the encryption byte from the client, or EncryptNotSup if not found.
    /// </summary>
    public byte ParseClientPreLogin(ReadOnlySpan<byte> payload)
    {
        byte clientEncryption = TdsConstants.EncryptNotSup;
        int offset = 0;

        while (offset < payload.Length)
        {
            byte token = payload[offset];
            if (token == TdsConstants.PreLoginTerminator)
                break;

            if (offset + 4 >= payload.Length)
                break;

            ushort dataOffset = BinaryPrimitives.ReadUInt16BigEndian(payload[(offset + 1)..]);
            ushort dataLength = BinaryPrimitives.ReadUInt16BigEndian(payload[(offset + 3)..]);
            offset += 5;

            if (token == TdsConstants.PreLoginEncryption && dataLength >= 1 && dataOffset < payload.Length)
            {
                clientEncryption = payload[dataOffset];
                _logger.LogDebug("Client requested encryption: 0x{Enc:X2}", clientEncryption);
            }
        }

        return clientEncryption;
    }

    /// <summary>
    /// Builds the server PRELOGIN response payload.
    /// Advertises VERSION, ENCRYPTION=NOT_SUP, INSTOPT, THREADID=0, MARS=0, then TERMINATOR.
    /// </summary>
    public byte[] BuildServerPreLoginResponse()
    {
        // Option entries: VERSION, ENCRYPTION, INSTOPT, THREADID, MARS, TERMINATOR
        // Each option header = 5 bytes (token + offset + length), terminator = 1 byte
        // 5 options * 5 bytes + 1 terminator = 26 bytes of option headers
        const int optionCount = 5;
        const int optionHeaderSize = optionCount * 5 + 1;

        // Data sizes: VERSION=6, ENCRYPTION=1, INSTOPT=1, THREADID=4, MARS=1
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

        // VERSION: 15.0.0.0 (SQL Server 2019-ish) + sub-build 0
        WriteOption(TdsConstants.PreLoginVersion, versionLen);
        buf[dataPos++] = 15;
        buf[dataPos++] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 2;
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 2;

        // ENCRYPTION: NOT_SUP
        WriteOption(TdsConstants.PreLoginEncryption, encryptionLen);
        buf[dataPos++] = TdsConstants.EncryptNotSup;

        // INSTOPT: single null byte
        WriteOption(TdsConstants.PreLoginInstOpt, instOptLen);
        buf[dataPos++] = 0x00;

        // THREADID: 0
        WriteOption(TdsConstants.PreLoginThreadId, threadIdLen);
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(dataPos), 0);
        dataPos += 4;

        // MARS: not supported
        WriteOption(TdsConstants.PreLoginMars, marsLen);
        buf[dataPos++] = 0x00;

        // TERMINATOR
        buf[headerPos] = TdsConstants.PreLoginTerminator;

        _logger.LogDebug("Built server PRELOGIN response ({Len} bytes)", totalLen);
        return buf;
    }
}
