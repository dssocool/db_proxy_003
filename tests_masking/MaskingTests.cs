using System.Text;
using Microsoft.Data.SqlClient;
using static MaskingProxyTests.MaskingTestSetup;

namespace MaskingProxyTests;

public static class MaskingTests
{
    public static void RunAllTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        var testCases = new (string Name, string Sql)[]
        {
            ("MASK_SELECT_ALL",
                $"SELECT * FROM {TableName} ORDER BY Id"),

            ("MASK_SELECT_NAME",
                $"SELECT Id, Name FROM {TableName} ORDER BY Id"),

            ("MASK_SELECT_SSN",
                $"SELECT Id, SSN FROM {TableName} ORDER BY Id"),

            ("MASK_SELECT_EMAIL",
                $"SELECT Id, Email FROM {TableName} ORDER BY Id"),

            ("MASK_SELECT_UNMASKED",
                $"SELECT Id, Age, Balance FROM {TableName} ORDER BY Id"),

            ("MASK_SELECT_WHERE",
                $"SELECT Name, Email FROM {TableName} WHERE Id = 1"),

            ("MASK_SELECT_ALIAS",
                $"SELECT Name AS FullName, Email AS ContactEmail FROM {TableName} WHERE Id = 1"),

            ("MASK_SELECT_NULL_ROW",
                $"SELECT Id, Name, SSN, Email FROM {TableName} WHERE Id = 5"),

            ("MASK_SELECT_MIXED",
                $"SELECT Id, Name, Age, SSN, Balance, Email FROM {TableName} ORDER BY Id"),
        };

        Console.WriteLine($"===== Running {testCases.Length} masking tests =====");
        Console.WriteLine();

        RunTestCases(connStr, testCases, results);
    }

    public static void RunAllTypesTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        var testCases = new (string Name, string Sql)[]
        {
            ("ALLTYPES_SELECT_ALL",
                $"SELECT * FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_STRINGS",
                $"SELECT Id, Name, SSN, Email FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_NUMERICS",
                $"SELECT Id, ColTinyInt, ColSmallInt, ColBigInt, ColBit, ColFloat, ColReal, ColMoney, ColSmallMoney FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_DATETIMES",
                $"SELECT Id, ColDate, ColTime, ColDateTime, ColSmallDateTime, ColDateTime2, ColDateTimeOffset FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_BINARY",
                $"SELECT Id, ColBinary, ColVarBinary, ColGuid FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_SPECIAL",
                $"SELECT Id, ColXml, ColVariant FROM {AllTypesTableName} ORDER BY Id"),

            ("ALLTYPES_SELECT_NULLS",
                $"SELECT * FROM {AllTypesTableName} WHERE Id = 3"),
        };

        Console.WriteLine($"===== Running {testCases.Length} all-types tests =====");
        Console.WriteLine();

        RunTestCases(connStr, testCases, results);
    }

    public static void RunMaxTypesTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        var testCases = new (string Name, string Sql)[]
        {
            ("MAX_SELECT_ALL",
                $"SELECT * FROM {MaxTypesTableName} ORDER BY Id"),

            ("MAX_SELECT_STRINGS",
                $"SELECT Id, Name, SSN, Email FROM {MaxTypesTableName} ORDER BY Id"),

            ("MAX_SELECT_BINARY",
                $"SELECT Id, ColVarBinMax FROM {MaxTypesTableName} ORDER BY Id"),

            ("MAX_SELECT_NULLS",
                $"SELECT * FROM {MaxTypesTableName} WHERE Id = 2"),

            ("MAX_SELECT_MIXED",
                $"SELECT Id, Name, ColSmallName FROM {MaxTypesTableName} ORDER BY Id"),
        };

        Console.WriteLine($"===== Running {testCases.Length} MAX-types tests =====");
        Console.WriteLine();

        RunTestCases(connStr, testCases, results);
    }

    static void RunTestCases(string connStr, (string Name, string Sql)[] testCases,
        List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        foreach (var (name, sql) in testCases)
        {
            try
            {
                var sb = new StringBuilder();
                using var conn = new SqlConnection(connStr);
                conn.Open();
                using var cmd = new SqlCommand(sql, conn);
                using var reader = cmd.ExecuteReader();

                int resultSetIndex = 0;
                do
                {
                    if (resultSetIndex > 0) sb.AppendLine("---NEXT_RESULTSET---");

                    var colNames = new List<string>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        colNames.Add(reader.GetName(i));
                    sb.AppendLine(string.Join("|", colNames));

                    var colTypes = new List<string>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        colTypes.Add(reader.GetFieldType(i)?.Name ?? "?");
                    sb.AppendLine(string.Join("|", colTypes));

                    while (reader.Read())
                    {
                        var vals = new List<string>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                vals.Add("NULL");
                            }
                            else
                            {
                                var value = reader.GetValue(i);
                                vals.Add(value is byte[] bytes
                                    ? "0x" + Convert.ToHexString(bytes)
                                    : value?.ToString() ?? "NULL");
                            }
                        }
                        sb.AppendLine(string.Join("|", vals));
                    }
                    resultSetIndex++;
                } while (reader.NextResult());

                results.Add((name, true, sb.ToString().TrimEnd(), null));
                Console.WriteLine($"  [PASS] {name}");
            }
            catch (Exception ex)
            {
                results.Add((name, false, "", ex.Message));
                Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
            }
        }
    }
}
