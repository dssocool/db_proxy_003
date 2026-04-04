using MaskingProxyTests;
using static MaskingProxyTests.MaskingTestSetup;

string mode = args.Length > 0 ? args[0] : "direct";

if (mode == "setup")
{
    SetupDatabase();
    return;
}

if (mode == "teardown")
{
    TeardownDatabase();
    return;
}

if (mode == "compare")
{
    var baseDir = AppContext.BaseDirectory;
    MaskingCompare.CompareResults(
        Path.Combine(baseDir, "results_direct.txt"),
        Path.Combine(baseDir, "results_proxy.txt"));
    return;
}

string connStr = mode == "proxy" ? ProxyConn : DirectConn;
string label = mode == "proxy" ? "PROXY" : "DIRECT";

Console.WriteLine($"===== Running masking tests against {label} ({connStr.Split(';')[0]}) =====");
Console.WriteLine();

var results = new List<(string Name, bool Ok, string Data, string? Error)>();

MaskingTests.RunAllTests(connStr, results);
MaskingTests.RunAllTypesTests(connStr, results);
MaskingTests.RunMaxTypesTests(connStr, results);

Console.WriteLine();
Console.WriteLine($"===== {label} Summary: {results.Count(r => r.Ok)} passed, {results.Count(r => !r.Ok)} failed out of {results.Count} =====");

var outputFile = Path.Combine(AppContext.BaseDirectory, $"results_{mode}.txt");
using (var writer = new StreamWriter(outputFile))
{
    foreach (var (name, ok, data, error) in results)
    {
        writer.WriteLine($"### {name}");
        writer.WriteLine($"STATUS: {(ok ? "PASS" : "FAIL")}");
        if (ok)
            writer.WriteLine(data);
        else
            writer.WriteLine($"ERROR: {error}");
        writer.WriteLine("###END###");
    }
}
Console.WriteLine($"Results saved to: {outputFile}");
