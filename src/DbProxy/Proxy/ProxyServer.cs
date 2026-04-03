using System.Net;
using System.Net.Sockets;
using DbProxy.Config;
using Microsoft.Extensions.Logging;

namespace DbProxy.Proxy;

public sealed class ProxyServer
{
    private readonly ProxyConfig _config;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    public ProxyServer(ProxyConfig config, ILoggerFactory loggerFactory)
    {
        _config = config;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<ProxyServer>();
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _config.ListenPort);
        listener.Start();
        _logger.LogInformation("TDS Proxy listening on port {Port}", _config.ListenPort);
        _logger.LogInformation("Backend: {Backend}", _config.BackendConnectionString);

        ct.Register(() => listener.Stop());

        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                _ = HandleClientAsync(client, ct);
            }
        }
        finally
        {
            listener.Stop();
            _logger.LogInformation("TDS Proxy stopped");
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        await using var session = new TdsClientSession(client, _config, _loggerFactory);
        try
        {
            await session.RunAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error in client session");
        }
    }
}
