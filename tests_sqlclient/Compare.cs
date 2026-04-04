namespace SqlProxyTests;

public static class ResultComparer
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
        "DATE_FUNCTIONS", "CAST_CONVERT", "VERSION_INFO",
        "GUID_NEWID", "DATE_COLUMN_FUNCTIONS", "DATETIMEOFFSET_OPS",
        "INFOMSG_PRINT", "INFOMSG_RAISERROR_LOW"
    };

    private static readonly HashSet<string> ErrorTests = new()
    {
        "ERR_SYNTAX", "ERR_CONVERSION", "ERR_PK_VIOLATION",
        "ERR_FK_VIOLATION", "ERR_TIMEOUT", "ERR_CANCELED",
        "ASYNC_CANCELLATION_TOKEN", "ASYNC_CANCEL_LONG_QUERY"
    };

    public static string NormalizeForComparison(string data, string testName)
    {
        var lines = data.Split('\n', StringSplitOptions.None)
            .Select(l => l.TrimEnd())
            .ToList();

        if (FullyDynamic.Contains(testName))
            return "[DYNAMIC_VALUE]";

        if (ErrorTests.Contains(testName))
        {
            var normalized = new List<string>();
            foreach (var line in lines)
            {
                if (line.StartsWith("ErrorNumber|") || line.StartsWith("Int32|") ||
                    line.StartsWith("ASSERT_SQLERROR|") ||
                    line == "ErrorType" || line == "String")
                {
                    normalized.Add(line);
                }
                else if (line.Contains('|'))
                {
                    var parts = line.Split('|');
                    if (parts.Length >= 3)
                        normalized.Add($"{parts[0]}|{parts[1]}|{parts[2]}");
                    else
                        normalized.Add(line);
                }
                else
                {
                    normalized.Add(line);
                }
            }
            return string.Join("\n", normalized);
        }

        return string.Join("\n", lines);
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
            ? "  COMPARISON: DIRECT vs PROXY (STRICT)"
            : "  COMPARISON: DIRECT vs PROXY");
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
