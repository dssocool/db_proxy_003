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
    private SqlConnection? _backendConnection;
    private string _database = "";
    private int _packetSize = TdsConstants.DefaultPacketSize;

    public TdsClientSession(TcpClient client, ProxyConfig config, ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _logger = loggerFactory.CreateLogger<TdsClientSession>();
        _stream = client.GetStream();
        _reader = new TdsPacketReader(_stream);
        _writer = new TdsPacketWriter(_stream);
        _preLogin = new PreLoginHandler(loggerFactory.CreateLogger<PreLoginHandler>());
        _login = new LoginHandler(loggerFactory.CreateLogger<LoginHandler>());
        _queryHandler = new QueryHandler(loggerFactory.CreateLogger<QueryHandler>());
        _responseBuilder = new ResponseBuilder(loggerFactory.CreateLogger<ResponseBuilder>());
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var endpoint = _client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _logger.LogInformation("Client connected: {Endpoint}", endpoint);

        try
        {
            if (!await HandlePreLoginAsync(ct))
                return;

            if (!await HandleLoginAsync(ct))
                return;

            await OpenBackendConnectionAsync(ct);

            await QueryLoopAsync(ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Session cancelled for {Endpoint}", endpoint);
        }
        catch (IOException ex) when (ex.InnerException is SocketException)
        {
            _logger.LogInformation("Client disconnected: {Endpoint}", endpoint);
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
        var msg = await _reader.ReadMessageAsync(ct);
        if (msg is null)
            return false;

        var (packetType, payload) = msg.Value;
        if (packetType != TdsConstants.PacketTypePreLogin)
        {
            _logger.LogWarning("Expected PRELOGIN (0x12), got 0x{Type:X2}", packetType);
            return false;
        }

        _preLogin.ParseClientPreLogin(payload);

        byte[] response = _preLogin.BuildServerPreLoginResponse();
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, response, ct);

        _logger.LogDebug("PRELOGIN handshake complete");
        return true;
    }

    private async Task<bool> HandleLoginAsync(CancellationToken ct)
    {
        var msg = await _reader.ReadMessageAsync(ct);
        if (msg is null)
            return false;

        var (packetType, payload) = msg.Value;
        if (packetType != TdsConstants.PacketTypeLogin7)
        {
            _logger.LogWarning("Expected LOGIN7 (0x10), got 0x{Type:X2}", packetType);
            return false;
        }

        var loginInfo = _login.ParseLogin7(payload);

        if (!_login.ValidateCredentials(loginInfo, _config))
        {
            _logger.LogWarning("Login failed for user {User}", loginInfo.UserName);
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
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, loginResponse, ct);

        _logger.LogInformation("Login successful for user {User}, database {Db}", loginInfo.UserName, _database);
        return true;
    }

    private async Task OpenBackendConnectionAsync(CancellationToken ct)
    {
        _backendConnection = new SqlConnection(_config.BackendConnectionString);
        await _backendConnection.OpenAsync(ct);
        _logger.LogInformation("Backend connection opened to {Server}", _backendConnection.DataSource);
    }

    private async Task QueryLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var msg = await _reader.ReadMessageAsync(ct);
            if (msg is null)
            {
                _logger.LogInformation("Client disconnected (EOF)");
                break;
            }

            var (packetType, payload) = msg.Value;

            switch (packetType)
            {
                case TdsConstants.PacketTypeSqlBatch:
                    await HandleSqlBatchAsync(payload, ct);
                    break;

                case TdsConstants.PacketTypeAttention:
                    _logger.LogDebug("Received ATTENTION signal, sending empty DONE");
                    await SendDoneAsync(TdsConstants.DoneFinal, 0, ct);
                    break;

                default:
                    _logger.LogWarning("Unsupported packet type 0x{Type:X2}, ignoring", packetType);
                    await SendErrorAndDoneAsync(
                        $"Unsupported TDS message type 0x{packetType:X2}", ct);
                    break;
            }
        }
    }

    private async Task HandleSqlBatchAsync(byte[] payload, CancellationToken ct)
    {
        string sql = _queryHandler.ParseSqlBatch(payload);

        if (_backendConnection is null || _backendConnection.State != System.Data.ConnectionState.Open)
        {
            await SendErrorAndDoneAsync("Backend connection is not available", ct);
            return;
        }

        using var cmd = new SqlCommand(sql, _backendConnection);
        cmd.CommandTimeout = 120;

        byte[] response = await _responseBuilder.BuildResponseFromCommandAsync(cmd, ct);
        await _writer.WriteMessageAsync(TdsConstants.PacketTypeTabularResult, response, ct);
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
}
