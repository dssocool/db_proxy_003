namespace DbProxy.Config;

public sealed class ProxyConfig
{
    public int ListenPort { get; set; } = 1433;
    public string SqlUsername { get; set; } = "proxyuser";
    public string SqlPassword { get; set; } = "proxypassword";
    public string BackendConnectionString { get; set; } = "";
}
