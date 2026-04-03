using DbProxy.Config;
using DbProxy.Proxy;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var config = new ProxyConfig();
configuration.GetSection("Proxy").Bind(config);

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

var logger = loggerFactory.CreateLogger("DbProxy");

logger.LogInformation("=== TDS Terminating DB Proxy ===");
logger.LogInformation("Listen port: {Port}", config.ListenPort);
logger.LogInformation("SQL auth user: {User}", config.SqlUsername);

if (string.IsNullOrEmpty(config.BackendConnectionString))
{
    logger.LogError("BackendConnectionString is not configured in appsettings.json");
    return 1;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    logger.LogInformation("Shutdown requested...");
};

var server = new ProxyServer(config, loggerFactory);
await server.RunAsync(cts.Token);

return 0;
