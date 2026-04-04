using System.Text.RegularExpressions;

namespace EfCoreProxyTests;

public static class EfCoreCompare
{
    public record TestResult(string Name, string Status, string Data);

    public static List<TestResult> ParseResultFile(string path)
    {
        var results = new List<TestResult>();
        var lines = File.ReadAllLines(path);
        string? name = null;
        string? status = null;
        var data = new List<string>();

        foreach (var line in lines)
        {
            if (line.StartsWith("### ") && !line.StartsWith("###END"))
            {
                name = line[4..];
                status = null;
                data.Clear();
            }
            else if (line.StartsWith("STATUS: "))
            {
                status = line[8..];
            }
            else if (line == "###END###")
            {
                results.Add(new TestResult(name!, status!, string.Join("\n", data).Trim()));
                name = null;
            }
            else
            {
                data.Add(line);
            }
        }
        return results;
    }

    private static readonly HashSet<string> FullyDynamic = new()
    {
        "EF_MIGRATION_APPLY", "EF_MIGRATION_SCRIPT", "EF_MODEL_METADATA"
    };

    private static readonly HashSet<string> ConcurrencyTests = new()
    {
        "EF_CONCURRENCY_SUCCESS", "EF_CONCURRENCY_CONFLICT"
    };

    private static readonly HashSet<string> CancellationTests = new()
    {
        "EF_ASYNC_CANCEL_MIDSTREAM"
    };

    public static string NormalizeForComparison(string data, string testName)
    {
        var lines = data.Split('\n', StringSplitOptions.None)
            .Select(l => l.TrimEnd())
            .ToList();

        if (FullyDynamic.Contains(testName))
            return "[DYNAMIC_VALUE]";

        var normalized = new List<string>();
        foreach (var line in lines)
        {
            var l = line;

            l = Regex.Replace(l, @"Id=\d+", "Id=[ID]");
            l = Regex.Replace(l, @"RowVersion=[0-9A-Fa-f]+", "RowVersion=[RV]");
            l = Regex.Replace(l, @"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?", "[TIMESTAMP]");
            l = Regex.Replace(l, @"AuditLogId=\d+", "AuditLogId=[ID]");

            if (ConcurrencyTests.Contains(testName))
                l = Regex.Replace(l, @"RowVersion=[^\s,}]+", "RowVersion=[RV]");

            if (CancellationTests.Contains(testName))
            {
                l = Regex.Replace(l, @"Consumed=\d+", "Consumed=[N]");
                l = Regex.Replace(l, @"ExceptionType=\w+", "ExceptionType=[CANCEL]");
            }

            normalized.Add(l);
        }
        return string.Join("\n", normalized);
    }

    public static void CompareResults(string directPath, string proxyPath, bool strict = false)
    {
        var directResults = ParseResultFile(directPath);
        var proxyResults = ParseResultFile(proxyPath);

        var directMap = directResults.ToDictionary(r => r.Name);
        var proxyMap = proxyResults.ToDictionary(r => r.Name);

        int matchCount = 0, mismatchCount = 0, missingCount = 0, bothFailCount = 0;

        Console.WriteLine("=" + new string('=', 79));
        Console.WriteLine(strict
            ? "  EF CORE COMPARISON: DIRECT vs PROXY (STRICT)"
            : "  EF CORE COMPARISON: DIRECT vs PROXY");
        Console.WriteLine("=" + new string('=', 79));
        Console.WriteLine();

        foreach (var dr in directResults)
        {
            if (!proxyMap.TryGetValue(dr.Name, out var pr))
            {
                Console.WriteLine($"  [MISSING] {dr.Name} - not found in proxy results");
                missingCount++;
                continue;
            }

            bool bothPass = dr.Status == "PASS" && pr.Status == "PASS";
            bool bothFail = dr.Status == "FAIL" && pr.Status == "FAIL";

            if (!bothPass && !bothFail)
            {
                Console.WriteLine($"  [MISMATCH] {dr.Name} - direct={dr.Status}, proxy={pr.Status}");
                if (pr.Status == "FAIL")
                    Console.WriteLine($"             Proxy error: {pr.Data}");
                mismatchCount++;
                continue;
            }

            if (bothFail)
            {
                if (strict)
                {
                    Console.WriteLine($"  [BOTH_FAIL] {dr.Name}");
                    Console.WriteLine($"             Direct: {dr.Data}");
                    Console.WriteLine($"             Proxy:  {pr.Data}");
                    bothFailCount++;
                }
                else
                {
                    Console.WriteLine($"  [MATCH]    {dr.Name} - both FAIL");
                    matchCount++;
                }
                continue;
            }

            var dNorm = NormalizeForComparison(dr.Data, dr.Name);
            var pNorm = NormalizeForComparison(pr.Data, dr.Name);

            if (dNorm == pNorm)
            {
                Console.WriteLine($"  [MATCH]    {dr.Name}");
                matchCount++;
            }
            else
            {
                Console.WriteLine($"  [DIFF]     {dr.Name}");
                var dLines = dr.Data.Split('\n');
                var pLines = pr.Data.Split('\n');
                int maxLines = Math.Max(dLines.Length, pLines.Length);
                for (int i = 0; i < Math.Min(maxLines, 8); i++)
                {
                    var dl = i < dLines.Length ? dLines[i].TrimEnd() : "(missing)";
                    var pl = i < pLines.Length ? pLines[i].TrimEnd() : "(missing)";
                    if (dl != pl)
                        Console.WriteLine($"    line {i}: DIRECT=[{dl}]  PROXY=[{pl}]");
                }
                if (maxLines > 8) Console.WriteLine($"    ... ({maxLines - 8} more lines)");
                mismatchCount++;
            }
        }

        Console.WriteLine();
        var summary = $"  TOTAL: {matchCount} match, {mismatchCount} differ, {missingCount} missing";
        if (bothFailCount > 0)
            summary += $", {bothFailCount} both-fail";
        summary += $" out of {directResults.Count}";
        Console.WriteLine(summary);
        Console.WriteLine("=" + new string('=', 79));
    }
}
