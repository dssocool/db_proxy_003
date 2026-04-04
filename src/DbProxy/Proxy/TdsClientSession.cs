using System.Net.Sockets;
using DbProxy.Config;
using DbProxy.Protocol;
using DbProxy.Tds;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbProxy.Proxy;

/// <summary>
/// Manages a single client connection's lifecycle:
/// PRELOGIN -> LOGIN7 -> query loop, with a backend SqlConnection using Entra auth.
/// </summary>
public sealed class TdsClientSession : IAsyncDisposable
{
    private readonly TcpClient _client;
    private readonly ProxyConfig _config;
    private readonly ILogger _logger;
    private readonly NetworkStream _stream;
    private readonly TdsPacketReader _reader;
    private readonly TdsPacketWriter _writer;
    private readonly PreLoginHandler _preLogin;
    private readonly LoginHandler _login;
    private readonly QueryHandler _queryHandler;
    private readonly ResponseBuilder _responseBuilder;
    private readonly RpcHandler _rpcHandler;
    private SqlConnection? _backendConnection;
    private string _database = "";
    private int _packetSize = TdsConstants.DefaultPacketSize;

    public TdsClientSession(TcpClient client, ProxyConfig config, ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _logger = loggerFactory.CreateLogger<TdsClientSession>();
        _stream = client.GetStream();

        var readerLogger = loggerFactory.CreateLogger("TdsPacketReader");
        var writerLogger = loggerFactory.CreateLogger("TdsPacketWriter");
        _reader = new TdsPacketReader(_stream, readerLogger);
        _writer = new TdsPacketWriter(_stream, writerLogger);
        _preLogin = new PreLoginHandler(loggerFactory.CreateLogger<PreLoginHandler>());
        _login = new LoginHandler(loggerFactory.CreateLogger<LoginHandler>());
        _queryHandler = new QueryHandler(loggerFactory.CreateLogger<QueryHandler>());
        _responseBuilder = new ResponseBuilder(loggerFactory.CreateLogger<ResponseBuilder>());
        _rpcHandler = new RpcHandler(loggerFactory.CreateLogger<RpcHandler>(), _responseBuilder);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var endpoint = _client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _logger.LogInformation("Client connected: {Endpoint}", endpoint);
        _logger.LogDebug("Client socket: LocalEndPoint={Local} NoDelay={NoDelay} ReceiveBufferSize={RecvBuf} SendBufferSize={SendBuf}",
            _client.Client.LocalEndPoint, _client.NoDelay, _client.ReceiveBufferSize, _client.SendBufferSize);

        try
        {
            _logger.LogDebug("=== Phase 1: PRELOGIN ===");
            if (!await HandlePreLoginAsync(ct))
            {
                _logger.LogWarning("PRELOGIN phase failed, closing connection");
                return;
            }
            _logger.LogDebug("=== PRELOGIN phase complete ===");

            _logger.LogDebug("=== Phase 2: LOGIN7 ===");
            if (!await HandleLoginAsync(ct))
            {
                _logger.LogWarning("LOGIN7 phase failed, closing connection");
                return;
            }
            _logger.LogDebug("=== LOGIN7 phase complete ===");

            _logger.LogDebug("=== Phase 3: Opening backend connection ===");
            await OpenBackendConnectionAsync(ct);
            _logger.LogDebug("=== Backend connection opened ===");

            _logger.LogDebug("=== Phase 4: Entering query loop ===");
            await QueryLoopAsync(ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Session cancelled for {Endpoint}", endpoint);
        }
        catch (IOException ex) when (ex.InnerException is SocketException sockEx)
        {
            _logger.LogInformation("Client disconnected: {Endpoint} (SocketError={Error})", endpoint, sockEx.SocketErrorCode);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Session error for {Endpoint}", endpoint);
        }
        finally
        {
            _logger.LogInformation("Session ended for {Endpoint}", endpoint);
        }
    }

    private async Task<bool> HandlePreLoginAsync(CancellationToken ct)
    {
        _logger.LogDebug("Waiting for client PRELOGIN message...");
        var msg = await _reader.ReadMessageAsync(ct);
        if (msg is null)
        {
            _logger.LogWarning("No data received (client closed before sending PRELOGIN)");
            return false;
        }

        var (packetType, payload) = msg.Value;
        _logger.LogDebug("Received message: Type=0x{Type:X2} PayloadSize={Size}", packetType, payload.Length);

        if (packetType != TdsConstants.PacketTypePreLogin)
        {
            _logger.LogWarning("Expected PRELOGIN (0x12), got 0x{Type:X2} — client may be trying TLS/SSL or unsupported protocol", packetType);
            return false;
        }

        var clientEncryption = _preLogin.ParseClientPreLogin(payload);
        _logger.LogDebug("Client encryption preference: 0x{Enc:X2} ({EncName})", clientEncryption, EncryptionName(clientEncryption));

        byte[] response = _preLogin.BuildServerPreLoginResponse();
        _logger.LogDebug("Sending server PRELOGIN response ({Len} bytes) with ENCRYPTION=NOT_SUP", response.Length);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, response, ct);

        _logger.LogInformation("PRELOGIN handshake complete (client encryption=0x{Enc:X2}, server=NOT_SUP)", clientEncryption);
        return true;
    }

    private async Task<bool> HandleLoginAsync(CancellationToken ct)
    {
        _logger.LogDebug("Waiting for client LOGIN7 message...");
        var msg = await _reader.ReadMessageAsync(ct);
        if (msg is null)
        {
            _logger.LogWarning("No data received (client closed before sending LOGIN7)");
            return false;
        }

        var (packetType, payload) = msg.Value;
        _logger.LogDebug("Received message: Type=0x{Type:X2} PayloadSize={Size}", packetType, payload.Length);

        if (packetType != TdsConstants.PacketTypeLogin7)
        {
            _logger.LogWarning("Expected LOGIN7 (0x10), got 0x{Type:X2} — SSMS may be trying SSL/TLS negotiation after PRELOGIN", packetType);

            if (packetType == 0x17 || packetType == 0x16 || packetType == 0x14)
            {
                _logger.LogWarning("Received what looks like a TLS/SSL handshake (ContentType=0x{Type:X2}). " +
                    "SSMS likely requires encryption. The proxy advertised ENCRYPT_NOT_SUP but the client is trying TLS anyway. " +
                    "This is the most likely cause of SSMS hanging.", packetType);
            }

            return false;
        }

        var loginInfo = _login.ParseLogin7(payload);

        _logger.LogDebug("LOGIN7 parsed: TdsVersion=0x{Ver:X8} PacketSize={PktSize} Host={Host} User={User} App={App} Server={Server} DB={Db}",
            loginInfo.TdsVersion, loginInfo.PacketSize, loginInfo.HostName, loginInfo.UserName,
            loginInfo.AppName, loginInfo.ServerName, loginInfo.Database);

        if (!_login.ValidateCredentials(loginInfo, _config))
        {
            _logger.LogWarning("Login failed for user '{User}' (credential mismatch)", loginInfo.UserName);
            byte[] errorResponse = _login.BuildLoginErrorResponse(
                $"Login failed for user '{loginInfo.UserName}'.");
            await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, errorResponse, ct);
            return false;
        }

        _database = string.IsNullOrEmpty(loginInfo.Database) ? "master" : loginInfo.Database;
        _packetSize = loginInfo.PacketSize > 0 && loginInfo.PacketSize <= TdsConstants.MaxPacketSize
            ? (int)loginInfo.PacketSize
            : TdsConstants.DefaultPacketSize;
        _writer.PacketSize = _packetSize;

        byte[] loginResponse = _login.BuildLoginResponse(_database, _packetSize);
        _logger.LogDebug("Sending LOGIN response ({Len} bytes): LOGINACK + ENVCHANGE(db={Db},pktsize={PktSize},lang=us_english) + DONE",
            loginResponse.Length, _database, _packetSize);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, loginResponse, ct);

        _logger.LogInformation("Login successful: User={User} App={App} Host={Host} DB={Db} PacketSize={PktSize}",
            loginInfo.UserName, loginInfo.AppName, loginInfo.HostName, _database, _packetSize);
        return true;
    }

    private async Task OpenBackendConnectionAsync(CancellationToken ct)
    {
        _logger.LogDebug("Opening backend SqlConnection...");
        _backendConnection = new SqlConnection(_config.BackendConnectionString);
        await _backendConnection.OpenAsync(ct);
        _logger.LogInformation("Backend connection opened: Server={Server} Database={Db} ServerVersion={Ver}",
            _backendConnection.DataSource, _backendConnection.Database, _backendConnection.ServerVersion);
    }

    private async Task QueryLoopAsync(CancellationToken ct)
    {
        int queryNum = 0;
        while (!ct.IsCancellationRequested)
        {
            _logger.LogDebug("Waiting for next client message...");
            var msg = await _reader.ReadMessageAsync(ct);
            if (msg is null)
            {
                _logger.LogInformation("Client disconnected (EOF)");
                break;
            }

            var (packetType, payload) = msg.Value;
            queryNum++;

            switch (packetType)
            {
                case TdsConstants.PacketTypeSqlBatch:
                    _logger.LogDebug("Query #{Num}: SQL_BATCH ({Len} bytes payload)", queryNum, payload.Length);
                    await HandleSqlBatchAsync(payload, ct);
                    _logger.LogDebug("Query #{Num}: Response sent", queryNum);
                    break;

                case TdsConstants.PacketTypeRpcRequest:
                    _logger.LogDebug("Query #{Num}: RPC_REQUEST ({Len} bytes payload)", queryNum, payload.Length);
                    await HandleRpcRequestAsync(payload, ct);
                    _logger.LogDebug("Query #{Num}: RPC response sent", queryNum);
                    break;

                case TdsConstants.PacketTypeAttention:
                    _logger.LogDebug("Query #{Num}: Received ATTENTION signal, sending empty DONE", queryNum);
                    await SendDoneAsync(TdsConstants.DoneFinal, 0, ct);
                    break;

                default:
                    _logger.LogWarning("Query #{Num}: Unsupported packet type 0x{Type:X2}", queryNum, packetType);
                    await SendErrorAndDoneAsync(
                        $"Unsupported TDS message type 0x{packetType:X2}", ct);
                    break;
            }
        }
    }

    private async Task HandleRpcRequestAsync(byte[] payload, CancellationToken ct)
    {
        if (_backendConnection is null || _backendConnection.State != System.Data.ConnectionState.Open)
        {
            _logger.LogWarning("Backend connection not available for RPC (State={State})",
                _backendConnection?.State.ToString() ?? "null");
            await SendErrorAndDoneAsync("Backend connection is not available", ct);
            return;
        }

        byte[] response = await _rpcHandler.HandleRpcAsync(payload, _backendConnection, ct);
        _logger.LogDebug("RPC response built: {Len} bytes", response.Length);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, response, ct);
    }

    private async Task HandleSqlBatchAsync(byte[] payload, CancellationToken ct)
    {
        string sql = _queryHandler.ParseSqlBatch(payload);
        _logger.LogDebug("Executing SQL: {Sql}", sql.Length > 500 ? sql[..500] + "..." : sql);

        if (_backendConnection is null || _backendConnection.State != System.Data.ConnectionState.Open)
        {
            _logger.LogWarning("Backend connection not available (State={State})",
                _backendConnection?.State.ToString() ?? "null");
            await SendErrorAndDoneAsync("Backend connection is not available", ct);
            return;
        }

        using var cmd = new SqlCommand(sql, _backendConnection);
        cmd.CommandTimeout = 120;

        byte[] response = await _responseBuilder.BuildResponseFromCommandAsync(cmd, ct);
        _logger.LogDebug("Response built: {Len} bytes", response.Length);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, response, ct);
        _logger.LogDebug("Response flushed to client. Socket connected={Connected} DataAvailable={DataAvailable}",
            _client.Connected, _stream.DataAvailable);
    }

    private async Task SendDoneAsync(ushort status, long rowCount, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        LoginHandler.WriteDoneToken(bw, status, rowCount);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, ms.ToArray(), ct);
    }

    private async Task SendErrorAndDoneAsync(string message, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);
        LoginHandler.WriteErrorToken(bw, 50000, 16, 1, message, "DbProxy", "", 0);
        LoginHandler.WriteDoneToken(bw, (ushort)(TdsConstants.DoneError | TdsConstants.DoneFinal), 0);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, ms.ToArray(), ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_backendConnection != null)
        {
            await _backendConnection.DisposeAsync();
            _backendConnection = null;
        }

        _stream.Dispose();
        _client.Dispose();
    }

    private static string EncryptionName(byte enc) => enc switch
    {
        TdsConstants.EncryptOff => "ENCRYPT_OFF",
        TdsConstants.EncryptOn => "ENCRYPT_ON",
        TdsConstants.EncryptNotSup => "ENCRYPT_NOT_SUP",
        TdsConstants.EncryptRequired => "ENCRYPT_REQ",
        _ => $"UNKNOWN(0x{enc:X2})",
    };
}
