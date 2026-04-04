using System.Text.RegularExpressions;

namespace MaskingProxyTests;

public static class MaskingCompare
{
    public record TestResult(string Name, string Status, string Data);

    private static readonly HashSet<string> MaskedColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "Name", "SSN", "Email"
    };

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

    public static void CompareResults(string directPath, string proxyPath)
    {
        var directResults = ParseResultFile(directPath);
        var proxyResults = ParseResultFile(proxyPath);

        var proxyMap = proxyResults.ToDictionary(r => r.Name);

        int passCount = 0, failCount = 0, missingCount = 0;

        Console.WriteLine("=" + new string('=', 79));
        Console.WriteLine("  MASKING COMPARISON: DIRECT vs PROXY (expect masked columns to DIFFER)");
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

            if (dr.Status != "PASS" || pr.Status != "PASS")
            {
                Console.WriteLine($"  [FAIL]    {dr.Name} - direct={dr.Status}, proxy={pr.Status}");
                if (dr.Status == "FAIL")
                    Console.WriteLine($"             Direct error: {dr.Data}");
                if (pr.Status == "FAIL")
                    Console.WriteLine($"             Proxy error: {pr.Data}");
                failCount++;
                continue;
            }

            var result = CompareTestData(dr.Name, dr.Data, pr.Data);
            if (result.Passed)
            {
                Console.WriteLine($"  [PASS]    {dr.Name}");
                passCount++;
            }
            else
            {
                Console.WriteLine($"  [FAIL]    {dr.Name}");
                foreach (var issue in result.Issues)
                    Console.WriteLine($"             {issue}");
                failCount++;
            }
        }

        Console.WriteLine();
        Console.WriteLine($"  TOTAL: {passCount} pass, {failCount} fail, {missingCount} missing out of {directResults.Count}");
        Console.WriteLine("=" + new string('=', 79));
    }

    record CompareResult(bool Passed, List<string> Issues);

    static CompareResult CompareTestData(string testName, string directData, string proxyData)
    {
        var issues = new List<string>();
        var dLines = directData.Split('\n').Select(l => l.TrimEnd()).ToArray();
        var pLines = proxyData.Split('\n').Select(l => l.TrimEnd()).ToArray();

        if (dLines.Length < 2 || pLines.Length < 2)
        {
            issues.Add("Insufficient data lines (need at least header + type row)");
            return new CompareResult(false, issues);
        }

        var colNames = dLines[0].Split('|');
        var proxyColNames = pLines[0].Split('|');

        if (colNames.Length != proxyColNames.Length)
        {
            issues.Add($"Column count mismatch: direct={colNames.Length}, proxy={proxyColNames.Length}");
            return new CompareResult(false, issues);
        }

        int directRowCount = dLines.Length - 2;
        int proxyRowCount = pLines.Length - 2;
        if (directRowCount != proxyRowCount)
        {
            issues.Add($"Row count mismatch: direct={directRowCount}, proxy={proxyRowCount}");
            return new CompareResult(false, issues);
        }

        var maskedIndices = new HashSet<int>();
        for (int c = 0; c < colNames.Length; c++)
        {
            if (MaskedColumns.Contains(colNames[c].Trim()))
                maskedIndices.Add(c);
        }

        for (int row = 2; row < dLines.Length; row++)
        {
            var dVals = dLines[row].Split('|');
            var pVals = pLines[row].Split('|');
            int rowNum = row - 1;

            if (dVals.Length != pVals.Length)
            {
                issues.Add($"Row {rowNum}: column count mismatch in data");
                continue;
            }

            for (int c = 0; c < dVals.Length; c++)
            {
                var dv = dVals[c].Trim();
                var pv = pVals[c].Trim();
                var col = colNames[c].Trim();

                if (maskedIndices.Contains(c))
                {
                    if (dv == "NULL" && pv == "NULL")
                        continue;

                    var dvNorm = dv.TrimEnd('\0', ' ');
                    var pvNorm = pv.TrimEnd('\0', ' ');
                    if (dvNorm == pvNorm)
                        issues.Add($"Row {rowNum}, column '{col}': expected masked (different) but values are identical: [{dvNorm}]");

                    ValidateMaskFormat(col, dv, pv, rowNum, issues);
                }
                else
                {
                    var dvNorm = dv.TrimEnd('\0');
                    var pvNorm = pv.TrimEnd('\0');
                    if (dvNorm != pvNorm)
                        issues.Add($"Row {rowNum}, column '{col}': unmasked column should match but differs: direct=[{dvNorm}] proxy=[{pvNorm}]");
                }
            }
        }

        return new CompareResult(issues.Count == 0, issues);
    }

    static void ValidateMaskFormat(string colName, string directVal, string proxyVal, int rowNum, List<string> issues)
    {
        if (directVal == "NULL" || proxyVal == "NULL")
            return;

        var proxyTrimmed = proxyVal.TrimEnd('\0', ' ');

        switch (colName.ToUpperInvariant())
        {
            case "SSN":
                if (!Regex.IsMatch(proxyTrimmed, @"^\*{3}-\*{2}-\d{4}$"))
                    issues.Add($"Row {rowNum}, column 'SSN': proxy value [{proxyTrimmed}] does not match expected mask pattern ***-**-NNNN");
                break;

            case "NAME":
                if (proxyTrimmed.Length < 2)
                    break;
                if (proxyTrimmed[0] != directVal.TrimEnd()[0])
                    issues.Add($"Row {rowNum}, column 'Name': first character should be preserved, direct=[{directVal.TrimEnd()[0]}] proxy=[{proxyTrimmed[0]}]");
                var rest = proxyTrimmed[1..];
                if (!rest.All(ch => ch == '*'))
                    issues.Add($"Row {rowNum}, column 'Name': remaining characters should be asterisks, got [{proxyTrimmed}]");
                break;

            case "EMAIL":
                if (proxyTrimmed.Length < 2)
                    break;
                if (proxyTrimmed[0] != directVal.TrimEnd()[0])
                    issues.Add($"Row {rowNum}, column 'Email': first character should be preserved, direct=[{directVal.TrimEnd()[0]}] proxy=[{proxyTrimmed[0]}]");
                if (!proxyTrimmed.Contains("***@***"))
                    issues.Add($"Row {rowNum}, column 'Email': proxy value [{proxyTrimmed}] should contain '***@***'");
                break;
        }
    }
}
