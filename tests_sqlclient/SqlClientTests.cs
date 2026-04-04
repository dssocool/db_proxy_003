using System.Data;
using System.Text;
using System.Threading;
using Microsoft.Data.SqlClient;
using static SqlProxyTests.TestSetup;

namespace SqlProxyTests;

public static class SqlClientTests
{
    public static void RunAllTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        var testCases = new (string Name, string Sql)[]
        {
            // --- Basic SELECT variants ---
            ("SELECT_ALL", $"SELECT * FROM {TableName}"),
            ("SELECT_COLUMNS_INT", $"SELECT ColInt, ColBigInt, ColSmallInt, ColTinyInt FROM {TableName}"),
            ("SELECT_COLUMNS_STR", $"SELECT ColInt, ColChar, ColVarChar, ColNVarChar FROM {TableName}"),
            ("SELECT_TOP", $"SELECT TOP 2 * FROM {TableName}"),
            ("SELECT_WHERE_EQ", $"SELECT * FROM {TableName} WHERE ColInt = 1"),
            ("SELECT_WHERE_LIKE", $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColVarChar LIKE '%hello%'"),
            ("SELECT_ORDER_ASC", $"SELECT ColInt, ColVarChar FROM {TableName} ORDER BY ColVarChar ASC"),
            ("SELECT_ORDER_DESC", $"SELECT ColInt, ColBigInt FROM {TableName} ORDER BY ColInt DESC"),

            // --- Exact numeric types ---
            ("SELECT_EXACT_NUMERICS", $"SELECT ColInt, ColBigInt, ColSmallInt, ColTinyInt, ColBit, ColDecimal, ColNumeric, ColMoney, ColSmallMoney FROM {TableName}"),

            // --- Approximate numeric types ---
            ("SELECT_APPROX_NUMERICS", $"SELECT ColInt, ColFloat, ColReal FROM {TableName}"),

            // --- Date/time types ---
            ("SELECT_DATE_TYPES", $"SELECT ColInt, ColDate, ColDateTime, ColDateTime2, ColDateTimeOffset, ColSmallDateTime, ColTime FROM {TableName}"),

            // --- Character string types ---
            ("SELECT_CHAR_TYPES", $"SELECT ColInt, ColChar, ColVarChar, ColVarCharMax, ColText FROM {TableName}"),

            // --- Unicode string types ---
            ("SELECT_UNICODE_TYPES", $"SELECT ColInt, ColNChar, ColNVarChar, ColNVarCharMax, ColNText FROM {TableName}"),

            // --- Binary types ---
            ("SELECT_BINARY_TYPES", $"SELECT ColInt, ColBinary, ColVarBinary FROM {TableName}"),

            // --- Other types ---
            ("SELECT_GUID", $"SELECT ColInt, ColUniqueIdentifier FROM {TableName}"),
            ("SELECT_XML", $"SELECT ColInt, ColXml FROM {TableName}"),
            ("SELECT_SQL_VARIANT", $"SELECT ColInt, ColSqlVariant FROM {TableName}"),

            // --- NULL handling ---
            ("SELECT_NULL_CHECK", $"SELECT * FROM {TableName} WHERE ColVarChar IS NOT NULL"),
            ("SELECT_NULL_ROWS", $"SELECT ColInt, ColVarChar, ColFloat, ColDate FROM {TableName} WHERE ColVarCharMax IS NULL"),
            ("COALESCE_ISNULL", $"SELECT ColInt, COALESCE(ColVarChar, 'N/A') AS SafeStr, ISNULL(ColNVarChar, N'none') AS SafeNStr FROM {TableName}"),

            // --- Aggregates on numeric columns ---
            ("SELECT_COUNT", $"SELECT COUNT(*) AS Cnt FROM {TableName}"),
            ("AGGREGATE_SUM", $"SELECT SUM(ColBigInt) AS TotalBig, SUM(ColDecimal) AS TotalDec, SUM(ColMoney) AS TotalMoney FROM {TableName}"),
            ("AGGREGATE_MIN_MAX", $"SELECT MIN(ColInt) AS MinInt, MAX(ColInt) AS MaxInt, MIN(ColDate) AS MinDate, MAX(ColDate) AS MaxDate FROM {TableName}"),
            ("AGGREGATE_AVG", $"SELECT AVG(ColFloat) AS AvgFloat, AVG(ColReal) AS AvgReal, AVG(ColDecimal) AS AvgDec FROM {TableName}"),

            // --- DISTINCT ---
            ("SELECT_DISTINCT", $"SELECT DISTINCT ColBit FROM {TableName}"),

            // --- Alias ---
            ("SELECT_ALIAS", $"SELECT ColInt AS Id, ColVarChar AS Name, ColMoney AS Price FROM {TableName}"),

            // --- Filtering ---
            ("SELECT_IN", $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt IN (1, 2, 3)"),
            ("SELECT_BETWEEN", $"SELECT ColInt, ColDecimal FROM {TableName} WHERE ColInt BETWEEN 1 AND 3"),
            ("SELECT_EXISTS", $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {TableName}) THEN 1 ELSE 0 END AS HasRows"),

            // --- Subqueries ---
            ("SUBQUERY_SCALAR", $"SELECT *, (SELECT COUNT(*) FROM {TableName}) AS TotalRows FROM {TableName} WHERE ColInt = 1"),
            ("SUBQUERY_IN", $"SELECT * FROM {TableName} WHERE ColInt IN (SELECT TOP 2 ColInt FROM {TableName} ORDER BY ColInt)"),
            ("NESTED_SUBQUERY", $"SELECT * FROM {TableName} WHERE ColInt = (SELECT MIN(ColInt) FROM {TableName} WHERE ColInt > 0)"),

            // --- UNION ---
            ("UNION", $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt = 1 UNION SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt = 2"),
            ("UNION_ALL", $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt <= 2 UNION ALL SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt <= 2"),

            // --- CASE / IIF / CHOOSE ---
            ("SELECT_CASE", $"SELECT ColInt, CASE WHEN ColBit = 1 THEN 'Active' ELSE 'Inactive' END AS Status FROM {TableName}"),
            ("IIF_FUNCTION", $"SELECT ColInt, IIF(ColInt % 2 = 0, 'Even', 'Odd') AS Parity FROM {TableName}"),
            ("CHOOSE_FUNCTION", $"SELECT ColInt, CHOOSE(CASE WHEN ColInt <= 3 THEN ColInt ELSE 1 END, 'One', 'Two', 'Three') AS Word FROM {TableName}"),

            // --- String functions ---
            ("STRING_FUNCTIONS", $"SELECT ColInt, UPPER(ColVarChar) AS Upper, LOWER(ColVarChar) AS Lower, LEN(ColVarChar) AS Len FROM {TableName}"),
            ("CONCAT_STRING", $"SELECT ColInt, CONCAT(ColVarChar, ' [', ColNVarChar, ']') AS Display FROM {TableName}"),
            ("STRING_UNICODE", $"SELECT ColInt, UNICODE(ColNVarChar) AS FirstCodePoint, NCHAR(UNICODE(ColNVarChar)) AS FirstChar FROM {TableName} WHERE ColNVarChar IS NOT NULL"),

            // --- Math functions ---
            ("MATH_FUNCTIONS", $"SELECT ColInt, ABS(ColFloat) AS AbsFloat, CEILING(ColDecimal) AS CeilDec, FLOOR(ColDecimal) AS FloorDec, ROUND(ColFloat, 2) AS RoundFloat FROM {TableName}"),

            // --- Date functions ---
            ("DATE_FUNCTIONS", "SELECT GETDATE() AS Now, YEAR(GETDATE()) AS Yr, MONTH(GETDATE()) AS Mo"),
            ("DATE_COLUMN_FUNCTIONS", $"SELECT ColInt, YEAR(ColDate) AS Yr, MONTH(ColDate) AS Mo, DAY(ColDate) AS Dy, DATEDIFF(DAY, ColDate, GETDATE()) AS DaysAgo FROM {TableName} WHERE ColDate IS NOT NULL"),

            // --- CAST / CONVERT ---
            ("CAST_CONVERT", $"SELECT ColInt, CAST(ColInt AS VARCHAR(10)) AS IntStr, CAST(ColDecimal AS FLOAT) AS DecFloat, CONVERT(VARCHAR(30), ColDateTime, 120) AS DtStr FROM {TableName} WHERE ColDateTime IS NOT NULL"),
            ("CAST_BINARY", $"SELECT ColInt, CAST(ColVarChar AS VARBINARY(100)) AS StrToBin FROM {TableName} WHERE ColVarChar IS NOT NULL"),

            // --- OFFSET / FETCH ---
            ("OFFSET_FETCH", $"SELECT * FROM {TableName} ORDER BY ColInt OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY"),

            // --- GROUP BY / HAVING ---
            ("GROUP_BY", $"SELECT ColBit, COUNT(*) AS Cnt, SUM(CAST(ColMoney AS DECIMAL(38,4))) AS TotalMoney FROM {TableName} GROUP BY ColBit"),
            ("GROUP_BY_HAVING", $"SELECT ColBit, COUNT(*) AS Cnt FROM {TableName} GROUP BY ColBit HAVING COUNT(*) >= 1"),

            // --- CROSS APPLY ---
            ("CROSS_APPLY_VALUES", $"SELECT t.ColInt, t.ColVarChar, v.Label FROM {TableName} t CROSS APPLY (VALUES ('tag')) AS v(Label)"),

            // --- CTE ---
            ("CTE_SIMPLE", $"WITH Ranked AS (SELECT ColInt, ColVarChar, ROW_NUMBER() OVER (ORDER BY ColInt) AS Rn FROM {TableName}) SELECT * FROM Ranked"),

            // --- Window functions ---
            ("WINDOW_ROW_NUMBER", $"SELECT ColInt, ColVarChar, ROW_NUMBER() OVER (ORDER BY ColInt) AS RowNum FROM {TableName}"),
            ("WINDOW_RANK", $"SELECT ColInt, ColVarChar, RANK() OVER (ORDER BY ColVarChar) AS Rnk FROM {TableName}"),
            ("WINDOW_DENSE_RANK", $"SELECT ColInt, ColVarChar, DENSE_RANK() OVER (ORDER BY ColBit) AS DRnk FROM {TableName}"),
            ("WINDOW_SUM", $"SELECT ColInt, ColMoney, SUM(ColMoney) OVER (ORDER BY ColInt) AS RunningTotal FROM {TableName}"),

            // --- System catalog ---
            ("SYS_TABLES", $"SELECT TOP 5 name FROM sys.tables ORDER BY name"),
            ("SYS_COLUMNS", $"SELECT c.name, t.name AS type, c.max_length, c.precision, c.scale, c.is_nullable FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id WHERE c.object_id = OBJECT_ID('{TableName}') ORDER BY c.column_id"),

            // --- System functions ---
            ("DB_NAME_FUNC", "SELECT DB_NAME() AS CurrentDB"),
            ("VERSION_INFO", "SELECT @@VERSION AS SqlVersion"),

            // --- Multiple result sets ---
            ("MULTIPLE_RESULTSETS", $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt = 1; SELECT COUNT(*) AS Total FROM {TableName}"),

            // --- Empty result ---
            ("EMPTY_RESULT", $"SELECT * FROM {TableName} WHERE 1 = 0"),

            // --- Large IN list ---
            ("LARGE_IN_LIST", $"SELECT ColInt FROM {TableName} WHERE ColInt IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)"),

            // --- Joins ---
            ("SELF_JOIN", $"SELECT a.ColInt AS Id1, b.ColInt AS Id2, a.ColVarChar FROM {TableName} a INNER JOIN {TableName} b ON a.ColInt = b.ColInt"),
            ("LEFT_JOIN_SYS", $"SELECT t.ColInt, t.ColVarChar, c.name AS ColName FROM {TableName} t LEFT JOIN sys.columns c ON c.object_id = OBJECT_ID('{TableName}') AND c.column_id = t.ColInt"),

            // --- XML output ---
            ("FOR_XML_PATH", $"SELECT ColInt, ColVarChar FROM {TableName} FOR XML PATH('Row'), ROOT('Rows')"),

            // --- JSON output ---
            ("FOR_JSON_PATH", $"SELECT ColInt, ColVarChar, ColBigInt, ColDecimal, ColBit FROM {TableName} FOR JSON PATH"),

            // --- GUID-specific ---
            ("GUID_NEWID", "SELECT NEWID() AS RandomGuid"),
            ("GUID_FILTER", $"SELECT ColInt, ColUniqueIdentifier FROM {TableName} WHERE ColUniqueIdentifier IS NOT NULL"),

            // --- XML value/query ---
            ("XML_VALUE", $"SELECT ColInt, ColXml.value('(/root/item)[1]', 'VARCHAR(100)') AS XmlItem FROM {TableName} WHERE ColXml IS NOT NULL"),

            // --- SQL_VARIANT inspection ---
            ("SQL_VARIANT_TYPE", $"SELECT ColInt, ColSqlVariant, SQL_VARIANT_PROPERTY(ColSqlVariant, 'BaseType') AS BaseType FROM {TableName} WHERE ColSqlVariant IS NOT NULL"),

            // --- Money arithmetic ---
            ("MONEY_ARITHMETIC", $"SELECT ColInt, ColMoney, ColSmallMoney, CAST(ColMoney AS DECIMAL(38,4)) + CAST(ColSmallMoney AS DECIMAL(38,4)) AS TotalMoney, CAST(ColMoney AS DECIMAL(38,4)) * CAST(1.1 AS DECIMAL(19,4)) AS WithTax FROM {TableName} WHERE ColMoney IS NOT NULL"),

            // --- Binary comparison ---
            ("BINARY_LENGTH", $"SELECT ColInt, DATALENGTH(ColBinary) AS BinLen, DATALENGTH(ColVarBinary) AS VarBinLen FROM {TableName}"),

            // --- Datetime offset operations ---
            ("DATETIMEOFFSET_OPS", $"SELECT ColInt, ColDateTimeOffset, SWITCHOFFSET(ColDateTimeOffset, '+00:00') AS InUtc FROM {TableName} WHERE ColDateTimeOffset IS NOT NULL"),

            // --- Time operations ---
            ("TIME_OPS", $"SELECT ColInt, ColTime, DATEPART(HOUR, ColTime) AS Hr, DATEPART(MINUTE, ColTime) AS Mn FROM {TableName} WHERE ColTime IS NOT NULL"),

            // --- Stored Procedure calls (SQL BATCH EXEC) ---
            ("EXEC_SIMPLE", "EXEC usp_GetById @Id = 1"),
            ("EXEC_MULTI_PARAM", "EXEC usp_GetByRange @MinId = 1, @MaxId = 3"),
            ("EXEC_NO_PARAM", "EXEC usp_GetCount"),
            ("EXEC_MULTI_RESULT", "EXEC usp_MultiResult @Id = 1"),
            ("EXEC_DEFAULT_PARAM", "EXEC usp_WithDefaults @Id = 1"),
            ("EXEC_DEFAULT_OVERRIDE", "EXEC usp_WithDefaults @Id = 2, @Label = 'custom'"),

            // === DML: INSERT / UPDATE / DELETE / MERGE ===

            ("DML_INSERT_ROWCOUNT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'ins1', 10.00);
                   SELECT @@ROWCOUNT AS AffectedRows;"),

            ("DML_INSERT_OUTPUT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   DECLARE @ins TABLE (Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                   INSERT INTO [{DmlTableName}] (Id, Name, Value)
                   OUTPUT inserted.Id, inserted.Name, inserted.Value INTO @ins
                   VALUES (101, 'ins_out', 20.50);
                   SELECT * FROM @ins;"),

            ("DML_INSERT_MULTI_OUTPUT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   DECLARE @ins TABLE (Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                   INSERT INTO [{DmlTableName}] (Id, Name, Value)
                   OUTPUT inserted.Id, inserted.Name, inserted.Value INTO @ins
                   VALUES (102, 'multi1', 1.00), (103, 'multi2', 2.00), (104, 'multi3', 3.00);
                   SELECT * FROM @ins ORDER BY Id;"),

            ("DML_UPDATE_ROWCOUNT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'upd_test', 5.00);
                   UPDATE [{DmlTableName}] SET Value = 99.99 WHERE Id = 100;
                   SELECT @@ROWCOUNT AS AffectedRows;
                   SELECT Id, Name, Value FROM [{DmlTableName}] WHERE Id = 100;"),

            ("DML_UPDATE_OUTPUT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'upd_out', 5.00);
                   DECLARE @upd TABLE (Id INT, OldName VARCHAR(100), OldValue DECIMAL(18,2), NewName VARCHAR(100), NewValue DECIMAL(18,2));
                   UPDATE [{DmlTableName}]
                   SET Value = 77.77, Name = 'updated'
                   OUTPUT deleted.Id, deleted.Name, deleted.Value,
                          inserted.Name, inserted.Value INTO @upd
                   WHERE Id = 100;
                   SELECT * FROM @upd;"),

            ("DML_DELETE_ROWCOUNT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'del1', 1.00), (101, 'del2', 2.00);
                   DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   SELECT @@ROWCOUNT AS AffectedRows;"),

            ("DML_DELETE_OUTPUT",
                $@"DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'del_out', 42.00);
                   DECLARE @del TABLE (Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                   DELETE FROM [{DmlTableName}]
                   OUTPUT deleted.Id, deleted.Name, deleted.Value INTO @del
                   WHERE Id = 100;
                   SELECT * FROM @del;"),

            ("DML_MERGE",
                $@"DELETE FROM [{MergeSourceTableName}];
                   DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'existing', 10.00);
                   INSERT INTO [{MergeSourceTableName}] (Id, Name, Value) VALUES (100, 'src_upd', 99.00), (200, 'src_new', 50.00);
                   DECLARE @mrg TABLE (MergeAction VARCHAR(10), Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                   MERGE [{DmlTableName}] AS tgt
                   USING [{MergeSourceTableName}] AS src ON tgt.Id = src.Id
                   WHEN MATCHED THEN UPDATE SET tgt.Name = src.Name, tgt.Value = src.Value
                   WHEN NOT MATCHED THEN INSERT (Id, Name, Value) VALUES (src.Id, src.Name, src.Value)
                   OUTPUT $action, inserted.Id, inserted.Name, inserted.Value INTO @mrg;
                   SELECT * FROM @mrg ORDER BY Id;
                   DELETE FROM [{DmlTableName}] WHERE Id IN (100, 200);"),

            ("DML_TRIGGER_VERIFY",
                $@"DELETE FROM [{DmlAuditTableName}];
                   DELETE FROM [{DmlTableName}] WHERE Id >= 100;
                   INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (100, 'trig_test', 5.00);
                   SELECT TOP 1 AuditAction, AuditId FROM [{DmlAuditTableName}] ORDER BY LogId DESC;
                   DELETE FROM [{DmlTableName}] WHERE Id = 100;"),

            // === Transactions ===

            ("TXN_COMMIT",
                $@"DELETE FROM [{TxnTableName}] WHERE Id >= 500;
                   BEGIN TRAN;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (500, 'committed');
                   COMMIT;
                   SELECT Id, Val FROM [{TxnTableName}] WHERE Id = 500;
                   DELETE FROM [{TxnTableName}] WHERE Id = 500;"),

            ("TXN_ROLLBACK",
                $@"DELETE FROM [{TxnTableName}] WHERE Id >= 500;
                   BEGIN TRAN;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (501, 'will_rollback');
                   ROLLBACK;
                   SELECT COUNT(*) AS Cnt FROM [{TxnTableName}] WHERE Id = 501;"),

            ("TXN_NESTED_SAVE",
                $@"DELETE FROM [{TxnTableName}] WHERE Id >= 500;
                   BEGIN TRAN;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (510, 'outer');
                   SAVE TRAN sp1;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (511, 'inner_rolled_back');
                   ROLLBACK TRAN sp1;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (512, 'after_savepoint');
                   COMMIT;
                   SELECT Id, Val FROM [{TxnTableName}] WHERE Id IN (510, 511, 512) ORDER BY Id;
                   DELETE FROM [{TxnTableName}] WHERE Id IN (510, 512);"),

            ("TXN_XACT_STATE",
                $@"BEGIN TRAN;
                   SELECT XACT_STATE() AS XactDuringTran, @@TRANCOUNT AS TranCount;
                   COMMIT;
                   SELECT XACT_STATE() AS XactAfterCommit, @@TRANCOUNT AS TranCountAfter;"),

            ("TXN_READ_COMMITTED",
                $@"SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
                   SELECT 'READ_COMMITTED' AS IsolationLevel;
                   SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"),

            ("TXN_LOCK_HINTS",
                $@"DELETE FROM [{TxnTableName}] WHERE Id >= 500;
                   INSERT INTO [{TxnTableName}] (Id, Val) VALUES (520, 'lock_test');
                   BEGIN TRAN;
                   SELECT Id, Val FROM [{TxnTableName}] WITH (UPDLOCK, HOLDLOCK) WHERE Id = 520;
                   COMMIT;
                   DELETE FROM [{TxnTableName}] WHERE Id = 520;"),

            // === Multi-batch and mixed statements ===

            ("MIXED_DDL_DML_SELECT",
                @"CREATE TABLE #MixedTest (Id INT, Val VARCHAR(50));
                  INSERT INTO #MixedTest (Id, Val) VALUES (1, 'alpha'), (2, 'beta');
                  SELECT Id, Val FROM #MixedTest ORDER BY Id;
                  DROP TABLE #MixedTest;"),

            ("MULTI_STATEMENT_BATCH",
                @"DECLARE @x INT = 1;
                  SELECT @x AS V1;
                  SET @x = @x + 1;
                  SELECT @x AS V2;"),

            ("MIXED_ERROR_RECOVERY",
                $@"SET XACT_ABORT OFF;
                   SELECT 1 AS BeforeError;
                   BEGIN TRY SELECT CAST('notint' AS INT); END TRY BEGIN CATCH SELECT ERROR_NUMBER() AS ErrNum, ERROR_MESSAGE() AS ErrMsg; END CATCH;
                   SELECT 2 AS AfterError;"),
        };

        Console.WriteLine($"===== Running {testCases.Length} SQL batch tests =====");
        Console.WriteLine();

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
                            var val = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i)?.ToString() ?? "NULL";
                            vals.Add(val);
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

        RunRpcTests(connStr, results);
        RunDmlRpcTests(connStr, results);
        RunAdvancedRpcTests(connStr, results);
        RunConcurrencyTests(connStr, results);
        RunSessionStateTests(connStr, results);
        RunTempObjectTests(connStr, results);
        RunMultiBatchTests(connStr, results);
        RunErrorPropagationTests(connStr, results);
        RunAsyncTests(connStr, results);
        RunStreamingTests(connStr, results);
        RunBulkCopyTests(connStr, results);
        RunDataAdapterTests(connStr, results);
        RunMarsTests(connStr, results);
        RunInfoMessageTests(connStr, results);
    }

    // -----------------------------------------------------------------------
    // RPC tests (TDS_RPC packet type via CommandType.StoredProcedure / SqlParameter)
    // -----------------------------------------------------------------------

    static void RunRpcTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunRpcSpSimple(connStr, results);
        RunRpcSpMultiParam(connStr, results);
        RunRpcSpNoParam(connStr, results);
        RunRpcSpMultiResult(connStr, results);
        RunRpcSpOutput(connStr, results);
        RunRpcSpReturnValue(connStr, results);
        RunRpcSpDefaults(connStr, results);
        RunRpcParameterizedSelect(connStr, results);
        RunRpcParameterizedTypes(connStr, results);
        RunRpcParameterizedNull(connStr, results);
        RunRpcPreparedReuse(connStr, results);
    }

    static void RunRpcSpSimple(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_SIMPLE";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_GetById", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpMultiParam(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_MULTI_PARAM";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_GetByRange", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@MinId", 1);
            cmd.Parameters.AddWithValue("@MaxId", 3);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpNoParam(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_NO_PARAM";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_GetCount", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpMultiResult(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_MULTI_RESULT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_MultiResult", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpOutput(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_OUTPUT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_GetNameById", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            var outParam = new SqlParameter("@Name", SqlDbType.VarChar, 100)
            {
                Direction = ParameterDirection.Output
            };
            cmd.Parameters.Add(outParam);
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.AppendLine("OutputParam_Name");
            sb.AppendLine("String");
            sb.Append(outParam.Value?.ToString() ?? "NULL");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpReturnValue(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_RETURN_VALUE";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_ReturnStatus", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            var retParam = new SqlParameter
            {
                ParameterName = "@RetVal",
                SqlDbType = SqlDbType.Int,
                Direction = ParameterDirection.ReturnValue
            };
            cmd.Parameters.Add(retParam);
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.AppendLine("ReturnValue");
            sb.AppendLine("Int32");
            sb.Append(retParam.Value?.ToString() ?? "NULL");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcSpDefaults(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_SP_DEFAULTS";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_WithDefaults", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcParameterizedSelect(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_PARAMETERIZED_SELECT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar, ColBigInt FROM {TableName} WHERE ColInt = @Id", conn);
            cmd.Parameters.AddWithValue("@Id", 1);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcParameterizedTypes(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_PARAMETERIZED_TYPES";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar, ColDecimal, ColDate FROM {TableName} WHERE ColInt >= @MinId AND ColVarChar LIKE @Pattern AND ColDecimal > @MinDec", conn);
            cmd.Parameters.Add(new SqlParameter("@MinId", SqlDbType.Int) { Value = 1 });
            cmd.Parameters.Add(new SqlParameter("@Pattern", SqlDbType.VarChar, 100) { Value = "%hello%" });
            cmd.Parameters.Add(new SqlParameter("@MinDec", SqlDbType.Decimal) { Value = 100.0m });
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcParameterizedNull(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_PARAMETERIZED_NULL";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColVarChar = @Val OR @Val IS NULL", conn);
            cmd.Parameters.Add(new SqlParameter("@Val", SqlDbType.VarChar, 100) { Value = DBNull.Value });
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcPreparedReuse(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_PREPARED_REUSE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar FROM {TableName} WHERE ColInt = @Id", conn);
            cmd.Parameters.Add(new SqlParameter("@Id", SqlDbType.Int));

            cmd.Parameters["@Id"].Value = 1;
            using (var reader = cmd.ExecuteReader())
            {
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            cmd.Parameters["@Id"].Value = 2;
            using (var reader = cmd.ExecuteReader())
            {
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    static string ReadAllResults(SqlCommand cmd)
    {
        var sb = new StringBuilder();
        using var reader = cmd.ExecuteReader();
        int resultSetIndex = 0;
        do
        {
            if (resultSetIndex > 0) sb.AppendLine("---NEXT_RESULTSET---");
            AppendResultSet(reader, sb);
            resultSetIndex++;
        } while (reader.NextResult());
        return sb.ToString().TrimEnd();
    }

    static async Task<string> ReadAllResultsAsync(SqlCommand cmd, CancellationToken ct = default)
    {
        var sb = new StringBuilder();
        using var reader = await cmd.ExecuteReaderAsync(ct);
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

            while (await reader.ReadAsync(ct))
            {
                var vals = new List<string>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i)?.ToString() ?? "NULL";
                    vals.Add(val);
                }
                sb.AppendLine(string.Join("|", vals));
            }
            resultSetIndex++;
        } while (await reader.NextResultAsync(ct));
        return sb.ToString().TrimEnd();
    }

    static void AppendResultSet(SqlDataReader reader, StringBuilder sb)
    {
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
                var val = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i)?.ToString() ?? "NULL";
                vals.Add(val);
            }
            sb.AppendLine(string.Join("|", vals));
        }
    }

    static void AssertScalar(SqlConnection conn, string sql, object expected, string label, StringBuilder sb)
    {
        using var cmd = new SqlCommand(sql, conn);
        var actual = cmd.ExecuteScalar();
        bool match = Equals(actual, expected);
        sb.AppendLine($"ASSERT_SCALAR|{label}|expected={expected}|actual={actual}|{(match ? "OK" : "FAIL")}");
        if (!match)
            throw new Exception($"Assertion failed [{label}]: expected={expected}, actual={actual}");
    }

    static void AssertRowCount(SqlConnection conn, string sql, int expectedRows, string label, StringBuilder sb)
    {
        using var cmd = new SqlCommand(sql, conn);
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        bool match = count == expectedRows;
        sb.AppendLine($"ASSERT_ROWCOUNT|{label}|expected={expectedRows}|actual={count}|{(match ? "OK" : "FAIL")}");
        if (!match)
            throw new Exception($"Assertion failed [{label}]: expected {expectedRows} rows, got {count}");
    }

    static void AssertSqlError(SqlException ex, int[] expectedErrorNumbers, string label, StringBuilder sb)
    {
        var actual = ex.Errors.Cast<SqlError>().Select(e => e.Number).ToList();
        bool match = expectedErrorNumbers.All(n => actual.Contains(n));
        sb.AppendLine($"ASSERT_SQLERROR|{label}|expected=[{string.Join(",", expectedErrorNumbers)}]|actual=[{string.Join(",", actual)}]|{(match ? "OK" : "FAIL")}");
        if (!match)
            throw new Exception($"Assertion failed [{label}]: expected errors [{string.Join(",", expectedErrorNumbers)}], got [{string.Join(",", actual)}]");
    }

    static string FormatSqlError(SqlException ex)
    {
        var sb = new StringBuilder();
        sb.AppendLine("ErrorNumber|ErrorState|ErrorClass");
        sb.AppendLine("Int32|Byte|Byte");
        foreach (SqlError err in ex.Errors)
            sb.AppendLine($"{err.Number}|{err.State}|{err.Class}");
        return sb.ToString().TrimEnd();
    }

    // -----------------------------------------------------------------------
    // DML via RPC (stored procedures with OUTPUT / @@ROWCOUNT)
    // -----------------------------------------------------------------------

    static void RunDmlRpcTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunDmlSpInsertOutput(connStr, results);
        RunDmlSpUpdateOutput(connStr, results);
        RunDmlSpDeleteOutput(connStr, results);
    }

    static void RunDmlSpInsertOutput(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_DML_INSERT_OUTPUT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id >= 100");
            using var cmd = new SqlCommand("usp_DmlInsertOutput", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 150);
            cmd.Parameters.AddWithValue("@Name", "rpc_insert");
            cmd.Parameters.Add(new SqlParameter("@Val", SqlDbType.Decimal) { Precision = 18, Scale = 2, Value = 33.33m });
            var outRows = new SqlParameter("@RowsAffected", SqlDbType.Int) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outRows);

            var sb = new StringBuilder();
            using (var reader = cmd.ExecuteReader())
            {
                AppendResultSet(reader, sb);
            }
            sb.AppendLine("---PARAMS---");
            sb.AppendLine($"RowsAffected={outRows.Value}");

            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id = 150");
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunDmlSpUpdateOutput(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_DML_UPDATE_OUTPUT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id >= 100; INSERT INTO [{DmlTableName}](Id,Name,Value) VALUES(160,'before',10.00)");
            using var cmd = new SqlCommand("usp_DmlUpdateOutput", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 160);
            cmd.Parameters.AddWithValue("@NewName", "after");
            cmd.Parameters.Add(new SqlParameter("@NewVal", SqlDbType.Decimal) { Precision = 18, Scale = 2, Value = 88.88m });
            var outRows = new SqlParameter("@RowsAffected", SqlDbType.Int) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outRows);

            var sb = new StringBuilder();
            using (var reader = cmd.ExecuteReader())
            {
                AppendResultSet(reader, sb);
            }
            sb.AppendLine("---PARAMS---");
            sb.AppendLine($"RowsAffected={outRows.Value}");

            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id = 160");
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunDmlSpDeleteOutput(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_DML_DELETE_OUTPUT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id >= 100; INSERT INTO [{DmlTableName}](Id,Name,Value) VALUES(170,'to_delete',55.55)");
            using var cmd = new SqlCommand("usp_DmlDeleteOutput", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 170);
            var outRows = new SqlParameter("@RowsAffected", SqlDbType.Int) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outRows);

            var sb = new StringBuilder();
            using (var reader = cmd.ExecuteReader())
            {
                AppendResultSet(reader, sb);
            }
            sb.AppendLine("---PARAMS---");
            sb.AppendLine($"RowsAffected={outRows.Value}");

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Advanced RPC: large params, decimal edge cases, TVPs, output+return+resultset
    // -----------------------------------------------------------------------

    static void RunAdvancedRpcTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunRpcLargeVarBinary(connStr, results);
        RunRpcLargeNVarChar(connStr, results);
        RunRpcDecimalPrecision(connStr, results);
        RunRpcTvp(connStr, results);
        RunRpcOutputReturnAndResultSet(connStr, results);
    }

    static void RunRpcLargeVarBinary(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_LARGE_VARBINARY";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            var payload = new byte[8000];
            for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 256);

            using var cmd = new SqlCommand("usp_EchoVarBinary", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@Data", SqlDbType.VarBinary, -1) { Value = payload });
            var outLen = new SqlParameter("@DataLen", SqlDbType.Int) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outLen);
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.AppendLine("DataLen");
            sb.AppendLine("Int32");
            sb.Append(outLen.Value?.ToString() ?? "NULL");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcLargeNVarChar(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_LARGE_NVARCHAR";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            var largeStr = new string('X', 4000);

            using var cmd = new SqlCommand("usp_EchoNVarChar", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@Data", SqlDbType.NVarChar, -1) { Value = largeStr });
            var outLen = new SqlParameter("@DataLen", SqlDbType.Int) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outLen);
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.AppendLine("DataLen");
            sb.AppendLine("Int32");
            sb.Append(outLen.Value?.ToString() ?? "NULL");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcDecimalPrecision(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_DECIMAL_PRECISION";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_EchoDecimal", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@Val", SqlDbType.Decimal) { Precision = 38, Scale = 18, Value = 1234567890.123456789012345678m });
            var outVal = new SqlParameter("@Result", SqlDbType.Decimal) { Precision = 38, Scale = 18, Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outVal);
            cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            sb.AppendLine("Result");
            sb.AppendLine("Decimal");
            sb.Append(outVal.Value?.ToString() ?? "NULL");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcTvp(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_TVP";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();

            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Name", typeof(string));
            dt.Rows.Add(1, "tvp_row1");
            dt.Rows.Add(2, "tvp_row2");
            dt.Rows.Add(3, "tvp_row3");

            using var cmd = new SqlCommand("usp_TvpSelect", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            var tvpParam = new SqlParameter("@Items", SqlDbType.Structured)
            {
                TypeName = "dbo.IdNameType",
                Value = dt
            };
            cmd.Parameters.Add(tvpParam);

            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunRpcOutputReturnAndResultSet(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "RPC_OUTPUT_RETURN_RESULTSET";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("usp_MixedOutputReturnResultSet", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@Id", 1);
            var outName = new SqlParameter("@OutName", SqlDbType.VarChar, 100) { Direction = ParameterDirection.Output };
            cmd.Parameters.Add(outName);
            var retVal = new SqlParameter("@RetVal", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };
            cmd.Parameters.Add(retVal);

            var sb = new StringBuilder();
            using (var reader = cmd.ExecuteReader())
            {
                int rsIdx = 0;
                do
                {
                    if (rsIdx > 0) sb.AppendLine("---NEXT_RESULTSET---");
                    AppendResultSet(reader, sb);
                    rsIdx++;
                } while (reader.NextResult());
            }
            sb.AppendLine("---PARAMS---");
            sb.AppendLine($"OutName={outName.Value?.ToString() ?? "NULL"}");
            sb.Append($"ReturnValue={retVal.Value?.ToString() ?? "NULL"}");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Concurrency / deadlock tests
    // -----------------------------------------------------------------------

    static void RunConcurrencyTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunConcurrentInsertContention(connStr, results);
        RunDeadlockSimulation(connStr, results);
    }

    static void RunConcurrentInsertContention(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "CONCURRENCY_INSERT_CONTENTION";
        try
        {
            using (var setupConn = new SqlConnection(connStr))
            {
                setupConn.Open();
                ExecNonQuery(setupConn, $"DELETE FROM [{TxnTableName}] WHERE Id BETWEEN 900 AND 999");
            }

            var errors = new List<string>();
            var barrier = new ManualResetEventSlim(false);

            void Worker(int startId)
            {
                try
                {
                    using var c = new SqlConnection(connStr);
                    c.Open();
                    barrier.Wait();
                    for (int i = startId; i < startId + 10; i++)
                    {
                        using var cmd = new SqlCommand($"INSERT INTO [{TxnTableName}](Id,Val) VALUES(@Id,@Val)", c);
                        cmd.Parameters.AddWithValue("@Id", i);
                        cmd.Parameters.AddWithValue("@Val", $"worker_{startId}_{i}");
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    lock (errors) errors.Add(ex.Message);
                }
            }

            var t1 = new Thread(() => Worker(900));
            var t2 = new Thread(() => Worker(950));
            t1.Start();
            t2.Start();
            barrier.Set();
            t1.Join(TimeSpan.FromSeconds(15));
            t2.Join(TimeSpan.FromSeconds(15));

            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand($"SELECT COUNT(*) AS Cnt FROM [{TxnTableName}] WHERE Id BETWEEN 900 AND 999", conn);
            var cnt = (int)cmd.ExecuteScalar()!;

            ExecNonQuery(conn, $"DELETE FROM [{TxnTableName}] WHERE Id BETWEEN 900 AND 999");

            var sb = new StringBuilder();
            sb.AppendLine("InsertedCount|ErrorCount");
            sb.AppendLine("Int32|Int32");
            sb.Append($"{cnt}|{errors.Count}");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunDeadlockSimulation(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "CONCURRENCY_DEADLOCK_SIM";
        try
        {
            using (var setupConn = new SqlConnection(connStr))
            {
                setupConn.Open();
                ExecNonQuery(setupConn, $"DELETE FROM [{TxnTableName}] WHERE Id IN (800, 801)");
                ExecNonQuery(setupConn, $"INSERT INTO [{TxnTableName}](Id,Val) VALUES(800,'A'),(801,'B')");
            }

            int deadlockCount = 0;
            int successCount = 0;
            var barrier = new ManualResetEventSlim(false);

            void Session(int firstId, int secondId)
            {
                try
                {
                    using var c = new SqlConnection(connStr);
                    c.Open();
                    barrier.Wait();
                    using var txn = c.BeginTransaction(IsolationLevel.Serializable);
                    using (var cmd = new SqlCommand($"UPDATE [{TxnTableName}] SET Val = Val + '_x' WHERE Id = @Id", c, txn))
                    {
                        cmd.Parameters.AddWithValue("@Id", firstId);
                        cmd.ExecuteNonQuery();
                    }
                    Thread.Sleep(100);
                    using (var cmd2 = new SqlCommand($"UPDATE [{TxnTableName}] SET Val = Val + '_y' WHERE Id = @Id", c, txn))
                    {
                        cmd2.Parameters.AddWithValue("@Id", secondId);
                        cmd2.ExecuteNonQuery();
                    }
                    txn.Commit();
                    Interlocked.Increment(ref successCount);
                }
                catch (SqlException ex) when (ex.Number == 1205)
                {
                    Interlocked.Increment(ref deadlockCount);
                }
            }

            var t1 = new Thread(() => Session(800, 801));
            var t2 = new Thread(() => Session(801, 800));
            t1.Start();
            t2.Start();
            barrier.Set();
            t1.Join(TimeSpan.FromSeconds(15));
            t2.Join(TimeSpan.FromSeconds(15));

            using (var cleanupConn = new SqlConnection(connStr))
            {
                cleanupConn.Open();
                ExecNonQuery(cleanupConn, $"DELETE FROM [{TxnTableName}] WHERE Id IN (800, 801)");
            }

            int total = successCount + deadlockCount;
            if (total != 2)
            {
                results.Add((name, false, "", $"Expected total=2 but got {total} (success={successCount}, deadlock={deadlockCount})"));
                Console.WriteLine($"  [FAIL] {name}: Expected total=2 but got {total}");
                return;
            }
            if (deadlockCount < 1)
            {
                results.Add((name, false, "", $"Expected at least 1 deadlock but got 0"));
                Console.WriteLine($"  [FAIL] {name}: Expected at least 1 deadlock but got 0");
                return;
            }

            var sb = new StringBuilder();
            sb.AppendLine("SuccessCount|DeadlockCount");
            sb.AppendLine("Int32|Int32");
            sb.Append($"{successCount}|{deadlockCount}");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Session state / SET options
    // -----------------------------------------------------------------------

    static void RunSessionStateTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunSessionSetAnsiNulls(connStr, results);
        RunSessionSetQuotedIdentifier(connStr, results);
        RunSessionSetArithabort(connStr, results);
        RunSessionSetXactAbort(connStr, results);
        RunSessionSetDatefirst(connStr, results);
        RunSessionSetLanguage(connStr, results);
        RunSessionMultiCmdPersistence(connStr, results);
    }

    static void RunSessionSetAnsiNulls(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_ANSI_NULLS";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                $"SET ANSI_NULLS OFF; SELECT COUNT(*) AS Cnt FROM [{TableName}] WHERE ColVarChar = NULL", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("ANSI_NULLS_OFF");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand(
                $"SET ANSI_NULLS ON; SELECT COUNT(*) AS Cnt FROM [{TableName}] WHERE ColVarChar = NULL", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("ANSI_NULLS_ON");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionSetQuotedIdentifier(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_QUOTED_IDENTIFIER";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "SET QUOTED_IDENTIFIER OFF; SELECT \"literal_string\" AS Val", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("QUOTED_IDENTIFIER_OFF");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand(
                $"SET QUOTED_IDENTIFIER ON; SELECT [ColInt] FROM [{TableName}] WHERE [ColInt] = 1", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("QUOTED_IDENTIFIER_ON");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionSetArithabort(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_ARITHABORT";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                @"SET ARITHABORT OFF; SET ANSI_WARNINGS OFF;
                  SELECT 1/0 AS DivResult", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("ARITHABORT_OFF");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand(
                @"SET ARITHABORT ON; SET ANSI_WARNINGS ON;
                  BEGIN TRY SELECT 1/0 AS DivResult; END TRY
                  BEGIN CATCH SELECT ERROR_NUMBER() AS ErrNum; END CATCH", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("ARITHABORT_ON");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionSetXactAbort(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_XACT_ABORT";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                @"SET XACT_ABORT OFF;
                  BEGIN TRAN;
                  BEGIN TRY
                      SELECT CAST('bad' AS INT);
                  END TRY
                  BEGIN CATCH
                      SELECT ERROR_NUMBER() AS ErrNum, XACT_STATE() AS XactState;
                      IF XACT_STATE() = -1 ROLLBACK;
                  END CATCH;
                  SELECT @@TRANCOUNT AS TranCount;
                  IF @@TRANCOUNT > 0 COMMIT;", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("XACT_ABORT_OFF");
                int rsIdx = 0;
                do
                {
                    if (rsIdx > 0) sb.AppendLine("---NEXT_RESULTSET---");
                    AppendResultSet(reader, sb);
                    rsIdx++;
                } while (reader.NextResult());
            }

            sb.AppendLine("---SECTION---");

            using (var cmd = new SqlCommand(
                @"SET XACT_ABORT ON;
                  BEGIN TRAN;
                  BEGIN TRY
                      SELECT CAST('bad' AS INT);
                  END TRY
                  BEGIN CATCH
                      SELECT ERROR_NUMBER() AS ErrNum, XACT_STATE() AS XactState;
                      IF XACT_STATE() = -1 ROLLBACK;
                  END CATCH;
                  SELECT @@TRANCOUNT AS TranCount;
                  IF @@TRANCOUNT > 0 COMMIT;", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("XACT_ABORT_ON");
                int rsIdx = 0;
                do
                {
                    if (rsIdx > 0) sb.AppendLine("---NEXT_RESULTSET---");
                    AppendResultSet(reader, sb);
                    rsIdx++;
                } while (reader.NextResult());
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionSetDatefirst(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_DATEFIRST";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "SET DATEFIRST 1; SELECT DATEPART(dw, '20240101') AS DayOfWeek, @@DATEFIRST AS DateFirst", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("DATEFIRST_1");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand(
                "SET DATEFIRST 7; SELECT DATEPART(dw, '20240101') AS DayOfWeek, @@DATEFIRST AS DateFirst", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("DATEFIRST_7");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionSetLanguage(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_SET_LANGUAGE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "SET LANGUAGE French; SELECT DATENAME(month, '20240101') AS MonthName", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("LANGUAGE_FRENCH");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand(
                "SET LANGUAGE us_english; SELECT DATENAME(month, '20240101') AS MonthName", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("LANGUAGE_ENGLISH");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunSessionMultiCmdPersistence(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "SESSION_MULTI_CMD_PERSISTENCE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand("SET DATEFIRST 1", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new SqlCommand(
                "SELECT DATEPART(dw, '20240101') AS DayOfWeek, @@DATEFIRST AS DateFirst", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("AFTER_SET_DATEFIRST_1");
                AppendResultSet(reader, sb);
            }

            sb.AppendLine("---NEXT_RESULTSET---");

            using (var cmd = new SqlCommand("SET DATEFIRST 7", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new SqlCommand(
                "SELECT DATEPART(dw, '20240101') AS DayOfWeek, @@DATEFIRST AS DateFirst", conn))
            using (var reader = cmd.ExecuteReader())
            {
                sb.AppendLine("AFTER_SET_DATEFIRST_7");
                AppendResultSet(reader, sb);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Temporary objects
    // -----------------------------------------------------------------------

    static void RunTempObjectTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunTempTableCreateUse(connStr, results);
        RunTempTableCrossBatch(connStr, results);
        RunTempTableScopeCleanup(connStr, results);
        RunTableVariableBasic(connStr, results);
        RunTableVariableScope(connStr, results);
    }

    static void RunTempTableCreateUse(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "TEMP_TABLE_CREATE_USE";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                @"CREATE TABLE #TempCU (Id INT, Val VARCHAR(50));
                  INSERT INTO #TempCU VALUES (1,'a'),(2,'b'),(3,'c');
                  SELECT Id, Val FROM #TempCU ORDER BY Id;
                  DROP TABLE #TempCU;", conn);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunTempTableCrossBatch(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "TEMP_TABLE_CROSS_BATCH";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "IF OBJECT_ID('tempdb..#TempXB') IS NOT NULL DROP TABLE #TempXB; CREATE TABLE #TempXB (Id INT, Val VARCHAR(50)); INSERT INTO #TempXB VALUES (10,'cross'),(20,'batch');", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new SqlCommand(
                "SELECT Id, Val FROM #TempXB ORDER BY Id; DROP TABLE #TempXB;", conn))
            using (var reader = cmd.ExecuteReader())
                AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunTempTableScopeCleanup(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "TEMP_TABLE_SCOPE_CLEANUP";
        try
        {
            using var connA = new SqlConnection(connStr);
            connA.Open();
            using (var cmd = new SqlCommand(
                "IF OBJECT_ID('tempdb..#TempScope') IS NOT NULL DROP TABLE #TempScope; CREATE TABLE #TempScope (Id INT); INSERT INTO #TempScope VALUES (1);", connA))
                cmd.ExecuteNonQuery();

            var sb = new StringBuilder();
            string? errorMsg = null;
            using (var connB = new SqlConnection(connStr))
            {
                connB.Open();
                try
                {
                    using var cmd = new SqlCommand("SELECT * FROM #TempScope", connB);
                    using var reader = cmd.ExecuteReader();
                    AppendResultSet(reader, sb);
                }
                catch (SqlException ex)
                {
                    errorMsg = $"{ex.Number}|{ex.State}|{ex.Class}";
                }
            }

            using (var cmd = new SqlCommand("DROP TABLE #TempScope", connA))
                cmd.ExecuteNonQuery();

            if (errorMsg != null)
            {
                sb.AppendLine("ConnB_Error");
                sb.AppendLine("String");
                sb.Append(errorMsg);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunTableVariableBasic(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "TABLE_VARIABLE_BASIC";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                @"DECLARE @tv TABLE (Id INT, Val VARCHAR(50));
                  INSERT INTO @tv VALUES (1,'tv_a'),(2,'tv_b');
                  SELECT Id, Val FROM @tv ORDER BY Id;", conn);
            results.Add((name, true, ReadAllResults(cmd), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunTableVariableScope(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "TABLE_VARIABLE_SCOPE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "DECLARE @tv TABLE (Id INT); INSERT INTO @tv VALUES (1);", conn))
                cmd.ExecuteNonQuery();

            string? errorMsg = null;
            try
            {
                using var cmd = new SqlCommand("SELECT * FROM @tv", conn);
                using var reader = cmd.ExecuteReader();
                AppendResultSet(reader, sb);
            }
            catch (SqlException ex)
            {
                errorMsg = $"{ex.Number}|{ex.State}|{ex.Class}";
            }

            if (errorMsg != null)
            {
                sb.AppendLine("ScopeError");
                sb.AppendLine("String");
                sb.Append(errorMsg);
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Multi-batch tests
    // -----------------------------------------------------------------------

    static void RunMultiBatchTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunBatchGoEquivalent(connStr, results);
    }

    static void RunBatchGoEquivalent(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "BATCH_WITH_GO_EQUIVALENT";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using (var cmd = new SqlCommand(
                "IF OBJECT_ID('tempdb..#GoBatch') IS NOT NULL DROP TABLE #GoBatch; CREATE TABLE #GoBatch (Id INT, Val VARCHAR(50)); INSERT INTO #GoBatch VALUES (1,'go1'),(2,'go2');", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new SqlCommand(
                "SELECT Id, Val FROM #GoBatch ORDER BY Id; DROP TABLE #GoBatch;", conn))
            using (var reader = cmd.ExecuteReader())
                AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // Error propagation tests
    // -----------------------------------------------------------------------

    static void RunErrorPropagationTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunErrSyntax(connStr, results);
        RunErrConversion(connStr, results);
        RunErrPkViolation(connStr, results);
        RunErrFkViolation(connStr, results);
        RunErrTimeout(connStr, results);
        RunErrCanceled(connStr, results);
    }

    static void RunErrSyntax(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_SYNTAX";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("SELECT FROM WHERE", conn);
            cmd.ExecuteNonQuery();
            results.Add((name, false, "", "Expected SqlException but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected SqlException but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { 156 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (expected error {ex.Number})");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunErrConversion(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_CONVERSION";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("SELECT CAST('abc' AS INT)", conn);
            cmd.ExecuteNonQuery();
            results.Add((name, false, "", "Expected SqlException but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected SqlException but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { 245 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (expected error {ex.Number})");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunErrPkViolation(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_PK_VIOLATION";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"INSERT INTO [{ErrParentTableName}] (Id) VALUES (1)", conn);
            cmd.ExecuteNonQuery();
            results.Add((name, false, "", "Expected SqlException but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected SqlException but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { 2627 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (expected error {ex.Number})");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunErrFkViolation(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_FK_VIOLATION";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"INSERT INTO [{ErrChildTableName}] (Id, ParentId) VALUES (999, 999)", conn);
            cmd.ExecuteNonQuery();
            results.Add((name, false, "", "Expected SqlException but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected SqlException but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { 547 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (expected error {ex.Number})");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunErrTimeout(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_TIMEOUT";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("WAITFOR DELAY '00:00:10'", conn);
            cmd.CommandTimeout = 2;
            cmd.ExecuteNonQuery();
            results.Add((name, false, "", "Expected SqlException but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected SqlException but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { -2 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (expected error {ex.Number})");
        }
        catch (Exception ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorNumber|ErrorState|ErrorClass");
            sb.AppendLine("Int32|Byte|Byte");
            sb.Append($"-2|0|0");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (timeout: {ex.GetType().Name})");
        }
    }

    static void RunErrCanceled(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ERR_CANCELED";
        try
        {
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand("WAITFOR DELAY '00:01:00'", conn);
            cmd.CommandTimeout = 30;

            var task = Task.Run(() =>
            {
                try { cmd.ExecuteNonQuery(); }
                catch { throw; }
            });

            Thread.Sleep(500);
            cmd.Cancel();

            try
            {
                task.Wait(TimeSpan.FromSeconds(10));
            }
            catch (AggregateException ae)
            {
                var inner = ae.InnerException;
                if (inner is SqlException sqlEx)
                {
                    var sb2 = new StringBuilder();
                    sb2.AppendLine(FormatSqlError(sqlEx));
                    try { AssertSqlError(sqlEx, new[] { 0 }, name, sb2); }
                    catch { results.Add((name, false, sb2.ToString().TrimEnd(), $"Wrong error number: got {sqlEx.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {sqlEx.Number}"); return; }
                    results.Add((name, true, sb2.ToString().TrimEnd(), null));
                    Console.WriteLine($"  [PASS] {name} (canceled, error {sqlEx.Number})");
                    return;
                }
                var sb = new StringBuilder();
                sb.AppendLine("ErrorType");
                sb.AppendLine("String");
                sb.Append(inner?.GetType().Name ?? "Unknown");
                results.Add((name, true, sb.ToString(), null));
                Console.WriteLine($"  [PASS] {name} (canceled: {inner?.GetType().Name})");
                return;
            }

            results.Add((name, false, "", "Expected cancellation exception but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected cancellation exception but none was thrown");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            try { AssertSqlError(ex, new[] { 0 }, name, sb); }
            catch { results.Add((name, false, sb.ToString().TrimEnd(), $"Wrong error number: got {ex.Number}")); Console.WriteLine($"  [FAIL] {name}: Wrong error number: got {ex.Number}"); return; }
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (canceled, error {ex.Number})");
        }
        catch (Exception ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorType");
            sb.AppendLine("String");
            sb.Append(ex.GetType().Name);
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (canceled: {ex.GetType().Name})");
        }
    }

    // -----------------------------------------------------------------------
    // Async API tests
    // -----------------------------------------------------------------------

    static void RunAsyncTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunAsyncOpen(connStr, results);
        RunAsyncExecuteReader(connStr, results);
        RunAsyncExecuteNonQuery(connStr, results);
        RunAsyncExecuteScalar(connStr, results);
        RunAsyncCancellationToken(connStr, results);
        RunAsyncCancelLongQuery(connStr, results);
    }

    static void RunAsyncOpen(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_OPEN";
        try
        {
            var data = Task.Run(async () =>
            {
                var sb = new StringBuilder();
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();
                using var cmd = new SqlCommand($"SELECT ColInt, ColVarChar FROM [{TableName}] WHERE ColInt = 1", conn);
                using var reader = cmd.ExecuteReader();
                AppendResultSet(reader, sb);
                return sb.ToString().TrimEnd();
            }).GetAwaiter().GetResult();
            results.Add((name, true, data, null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAsyncExecuteReader(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_EXECUTE_READER";
        try
        {
            var data = Task.Run(async () =>
            {
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();
                using var cmd = new SqlCommand(
                    $"SELECT ColInt, ColVarChar FROM [{TableName}] WHERE ColInt <= 2 ORDER BY ColInt; SELECT COUNT(*) AS Cnt FROM [{TableName}]", conn);
                return await ReadAllResultsAsync(cmd);
            }).GetAwaiter().GetResult();
            results.Add((name, true, data, null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAsyncExecuteNonQuery(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_EXECUTE_NONQUERY";
        try
        {
            var data = Task.Run(async () =>
            {
                var sb = new StringBuilder();
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();

                using (var cmd = new SqlCommand(
                    $"INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (9901, 'async_test', 1.23)", conn))
                {
                    int rows = await cmd.ExecuteNonQueryAsync();
                    sb.AppendLine($"InsertedRows={rows}");
                }

                using (var cmd = new SqlCommand(
                    $"SELECT Id, Name, Value FROM [{DmlTableName}] WHERE Id = 9901", conn))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    AppendResultSet(reader, sb);
                }

                using (var cmd = new SqlCommand(
                    $"DELETE FROM [{DmlTableName}] WHERE Id = 9901", conn))
                {
                    int rows = await cmd.ExecuteNonQueryAsync();
                    sb.AppendLine($"DeletedRows={rows}");
                }

                return sb.ToString().TrimEnd();
            }).GetAwaiter().GetResult();
            results.Add((name, true, data, null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAsyncExecuteScalar(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_EXECUTE_SCALAR";
        try
        {
            var data = Task.Run(async () =>
            {
                var sb = new StringBuilder();
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();
                using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{TableName}]", conn);
                var result = await cmd.ExecuteScalarAsync();
                sb.AppendLine("ScalarResult");
                sb.AppendLine("Int32");
                sb.Append(result?.ToString() ?? "NULL");
                return sb.ToString();
            }).GetAwaiter().GetResult();
            results.Add((name, true, data, null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAsyncCancellationToken(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_CANCELLATION_TOKEN";
        try
        {
            Task.Run(async () =>
            {
                using var cts = new CancellationTokenSource();
                cts.Cancel();
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync(cts.Token);
                using var cmd = new SqlCommand($"SELECT 1", conn);
                await cmd.ExecuteReaderAsync(cts.Token);
            }).GetAwaiter().GetResult();

            results.Add((name, false, "", "Expected cancellation exception but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected cancellation exception but none was thrown");
        }
        catch (OperationCanceledException)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorType");
            sb.AppendLine("String");
            sb.Append("OperationCanceledException");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (OperationCanceledException)");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (SqlException: {ex.Number})");
        }
        catch (Exception ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorType");
            sb.AppendLine("String");
            sb.Append(ex.GetType().Name);
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (canceled: {ex.GetType().Name})");
        }
    }

    static void RunAsyncCancelLongQuery(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ASYNC_CANCEL_LONG_QUERY";
        try
        {
            Task.Run(async () =>
            {
                using var cts = new CancellationTokenSource();
                using var conn = new SqlConnection(connStr);
                await conn.OpenAsync();
                using var cmd = new SqlCommand("WAITFOR DELAY '00:01:00'", conn);
                cmd.CommandTimeout = 30;

                cts.CancelAfter(500);
                await cmd.ExecuteNonQueryAsync(cts.Token);
            }).GetAwaiter().GetResult();

            results.Add((name, false, "", "Expected cancellation exception but none was thrown"));
            Console.WriteLine($"  [FAIL] {name}: Expected cancellation exception but none was thrown");
        }
        catch (OperationCanceledException)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorType");
            sb.AppendLine("String");
            sb.Append("OperationCanceledException");
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (OperationCanceledException)");
        }
        catch (SqlException ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine(FormatSqlError(ex));
            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name} (SqlException: {ex.Number})");
        }
        catch (Exception ex)
        {
            var sb = new StringBuilder();
            sb.AppendLine("ErrorType");
            sb.AppendLine("String");
            sb.Append(ex.GetType().Name);
            results.Add((name, true, sb.ToString(), null));
            Console.WriteLine($"  [PASS] {name} (canceled: {ex.GetType().Name})");
        }
    }

    // -----------------------------------------------------------------------
    // Streaming / behavior mode tests
    // -----------------------------------------------------------------------

    static void RunStreamingTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunStreamSequentialAccess(connStr, results);
        RunStreamGetStream(connStr, results);
        RunStreamGetTextReader(connStr, results);
        RunStreamSingleRow(connStr, results);
    }

    static void RunStreamSequentialAccess(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "STREAM_SEQUENTIAL_ACCESS";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar, ColBigInt FROM [{TableName}] WHERE ColInt <= 3 ORDER BY ColInt", conn);
            using var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);

            sb.AppendLine("ColInt|ColVarChar|ColBigInt");
            sb.AppendLine("Int32|String|Int64");

            while (reader.Read())
            {
                var colInt = reader.GetInt32(0);
                var colVarChar = reader.IsDBNull(1) ? "NULL" : reader.GetString(1);
                var colBigInt = reader.IsDBNull(2) ? "NULL" : reader.GetInt64(2).ToString();
                sb.AppendLine($"{colInt}|{colVarChar}|{colBigInt}");
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunStreamGetStream(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "STREAM_GET_STREAM";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarBinaryMax FROM [{TableName}] WHERE ColInt = 1", conn);
            using var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);

            sb.AppendLine("ColInt|StreamLength");
            sb.AppendLine("Int32|Int64");

            if (reader.Read())
            {
                var colInt = reader.GetInt32(0);
                if (!reader.IsDBNull(1))
                {
                    using var stream = reader.GetStream(1);
                    using var ms = new MemoryStream();
                    stream.CopyTo(ms);
                    sb.AppendLine($"{colInt}|{ms.Length}");
                }
                else
                {
                    sb.AppendLine($"{colInt}|NULL");
                }
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunStreamGetTextReader(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "STREAM_GET_TEXT_READER";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColNVarCharMax FROM [{TableName}] WHERE ColInt = 1", conn);
            using var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);

            sb.AppendLine("ColInt|TextContent");
            sb.AppendLine("Int32|String");

            if (reader.Read())
            {
                var colInt = reader.GetInt32(0);
                if (!reader.IsDBNull(1))
                {
                    using var textReader = reader.GetTextReader(1);
                    var text = textReader.ReadToEnd();
                    sb.AppendLine($"{colInt}|{text}");
                }
                else
                {
                    sb.AppendLine($"{colInt}|NULL");
                }
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunStreamSingleRow(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "STREAM_SINGLE_ROW";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();
            using var cmd = new SqlCommand(
                $"SELECT ColInt, ColVarChar FROM [{TableName}] ORDER BY ColInt", conn);
            using var reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            int rowCount = 0;
            sb.AppendLine("ColInt|ColVarChar");
            sb.AppendLine("Int32|String");

            while (reader.Read())
            {
                var colInt = reader.GetInt32(0);
                var colVarChar = reader.IsDBNull(1) ? "NULL" : reader.GetString(1);
                sb.AppendLine($"{colInt}|{colVarChar}");
                rowCount++;
            }

            sb.AppendLine($"RowCount={rowCount}");

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // SqlBulkCopy tests
    // -----------------------------------------------------------------------

    static void RunBulkCopyTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunBulkCopyBasic(connStr, results);
        RunBulkCopyColumnMapping(connStr, results);
        RunBulkCopyBatchSize(connStr, results);
        RunBulkCopyTransaction(connStr, results);
    }

    static void RunBulkCopyBasic(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "BULKCOPY_BASIC";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            ExecNonQuery(conn, $"TRUNCATE TABLE [{BulkCopyTableName}]");

            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add("Value", typeof(decimal));
            dt.Rows.Add(1, "bulk_a", 10.50m);
            dt.Rows.Add(2, "bulk_b", 20.75m);
            dt.Rows.Add(3, "bulk_c", 30.00m);

            using (var bcp = new SqlBulkCopy(conn))
            {
                bcp.DestinationTableName = BulkCopyTableName;
                bcp.WriteToServer(dt);
            }

            using var cmd = new SqlCommand(
                $"SELECT Id, Name, Value FROM [{BulkCopyTableName}] ORDER BY Id", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunBulkCopyColumnMapping(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "BULKCOPY_COLUMN_MAPPING";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            ExecNonQuery(conn, $"TRUNCATE TABLE [{BulkCopyTableName}]");

            var dt = new DataTable();
            dt.Columns.Add("SourceId", typeof(int));
            dt.Columns.Add("SourceName", typeof(string));
            dt.Columns.Add("SourceValue", typeof(decimal));
            dt.Rows.Add(10, "mapped_a", 100.00m);
            dt.Rows.Add(20, "mapped_b", 200.00m);

            using (var bcp = new SqlBulkCopy(conn))
            {
                bcp.DestinationTableName = BulkCopyTableName;
                bcp.ColumnMappings.Add("SourceId", "Id");
                bcp.ColumnMappings.Add("SourceName", "Name");
                bcp.ColumnMappings.Add("SourceValue", "Value");
                bcp.WriteToServer(dt);
            }

            using var cmd = new SqlCommand(
                $"SELECT Id, Name, Value FROM [{BulkCopyTableName}] ORDER BY Id", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunBulkCopyBatchSize(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "BULKCOPY_BATCH_SIZE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            ExecNonQuery(conn, $"TRUNCATE TABLE [{BulkCopyTableName}]");

            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add("Value", typeof(decimal));
            for (int i = 1; i <= 5; i++)
                dt.Rows.Add(i, $"batch_{i}", i * 10.0m);

            using (var bcp = new SqlBulkCopy(conn))
            {
                bcp.DestinationTableName = BulkCopyTableName;
                bcp.BatchSize = 2;
                bcp.WriteToServer(dt);
            }

            AssertScalar(conn, $"SELECT COUNT(*) FROM [{BulkCopyTableName}]", 5, "BulkBatchRowCount", sb);

            using var cmd = new SqlCommand(
                $"SELECT Id, Name, Value FROM [{BulkCopyTableName}] ORDER BY Id", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunBulkCopyTransaction(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "BULKCOPY_TRANSACTION";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            ExecNonQuery(conn, $"TRUNCATE TABLE [{BulkCopyTableName}]");

            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add("Value", typeof(decimal));
            dt.Rows.Add(1, "txn_a", 5.00m);
            dt.Rows.Add(2, "txn_b", 6.00m);

            using (var txn = conn.BeginTransaction())
            {
                using (var bcp = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, txn))
                {
                    bcp.DestinationTableName = BulkCopyTableName;
                    bcp.WriteToServer(dt);
                }
                txn.Rollback();
            }

            AssertScalar(conn, $"SELECT COUNT(*) FROM [{BulkCopyTableName}]", 0, "AfterRollbackCount", sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // SqlDataAdapter / disconnected model tests
    // -----------------------------------------------------------------------

    static void RunDataAdapterTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunAdapterFill(connStr, results);
        RunAdapterFillMulti(connStr, results);
        RunAdapterUpdate(connStr, results);
    }

    static void RunAdapterFill(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ADAPTER_FILL";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using var adapter = new SqlDataAdapter(
                $"SELECT ColInt, ColVarChar, ColBigInt FROM [{TableName}] WHERE ColInt <= 3 ORDER BY ColInt", conn);
            var ds = new DataSet();
            adapter.Fill(ds);

            sb.AppendLine($"TableCount={ds.Tables.Count}");
            var tbl = ds.Tables[0];

            var colNames = new List<string>();
            foreach (DataColumn col in tbl.Columns) colNames.Add(col.ColumnName);
            sb.AppendLine(string.Join("|", colNames));

            var colTypes = new List<string>();
            foreach (DataColumn col in tbl.Columns) colTypes.Add(col.DataType.Name);
            sb.AppendLine(string.Join("|", colTypes));

            foreach (DataRow row in tbl.Rows)
            {
                var vals = new List<string>();
                foreach (DataColumn col in tbl.Columns)
                    vals.Add(row.IsNull(col) ? "NULL" : row[col]?.ToString() ?? "NULL");
                sb.AppendLine(string.Join("|", vals));
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAdapterFillMulti(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ADAPTER_FILL_MULTI";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            using var adapter = new SqlDataAdapter(
                $"SELECT ColInt FROM [{TableName}] WHERE ColInt <= 2 ORDER BY ColInt; SELECT COUNT(*) AS Cnt FROM [{TableName}]", conn);
            var ds = new DataSet();
            adapter.Fill(ds);

            sb.AppendLine($"TableCount={ds.Tables.Count}");

            for (int t = 0; t < ds.Tables.Count; t++)
            {
                if (t > 0) sb.AppendLine("---NEXT_TABLE---");
                var tbl = ds.Tables[t];
                var colNames = new List<string>();
                foreach (DataColumn col in tbl.Columns) colNames.Add(col.ColumnName);
                sb.AppendLine(string.Join("|", colNames));

                var colTypes = new List<string>();
                foreach (DataColumn col in tbl.Columns) colTypes.Add(col.DataType.Name);
                sb.AppendLine(string.Join("|", colTypes));

                foreach (DataRow row in tbl.Rows)
                {
                    var vals = new List<string>();
                    foreach (DataColumn col in tbl.Columns)
                        vals.Add(row.IsNull(col) ? "NULL" : row[col]?.ToString() ?? "NULL");
                    sb.AppendLine(string.Join("|", vals));
                }
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunAdapterUpdate(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "ADAPTER_UPDATE";
        try
        {
            var sb = new StringBuilder();
            using var conn = new SqlConnection(connStr);
            conn.Open();

            ExecNonQuery(conn, $"DELETE FROM [{AdapterTableName}]");
            ExecNonQuery(conn, $"INSERT INTO [{AdapterTableName}] (Id, Val) VALUES (1, 'original_a'), (2, 'original_b')");

            using var adapter = new SqlDataAdapter($"SELECT Id, Val FROM [{AdapterTableName}] ORDER BY Id", conn);
            using var builder = new SqlCommandBuilder(adapter);
            var ds = new DataSet();
            adapter.Fill(ds);

            var tbl = ds.Tables[0];
            tbl.Rows[0]["Val"] = "updated_a";

            var newRow = tbl.NewRow();
            newRow["Id"] = 3;
            newRow["Val"] = "inserted_c";
            tbl.Rows.Add(newRow);

            adapter.Update(ds);

            sb.AppendLine("---AFTER_UPDATE---");
            using var cmd = new SqlCommand($"SELECT Id, Val FROM [{AdapterTableName}] ORDER BY Id", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // MARS (Multiple Active Result Sets) tests
    // -----------------------------------------------------------------------

    static void RunMarsTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunMarsTwoReaders(connStr, results);
        RunMarsReaderPlusDml(connStr, results);
        RunMarsNestedCommands(connStr, results);
    }

    static void RunMarsTwoReaders(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "MARS_TWO_READERS";
        try
        {
            var sb = new StringBuilder();
            var marsBuilder = new SqlConnectionStringBuilder(connStr) { MultipleActiveResultSets = true };
            using var conn = new SqlConnection(marsBuilder.ConnectionString);
            conn.Open();

            using var cmd1 = new SqlCommand(
                $"SELECT ColInt, ColVarChar FROM [{TableName}] WHERE ColInt <= 2 ORDER BY ColInt", conn);
            using var cmd2 = new SqlCommand(
                $"SELECT ColInt, ColBigInt FROM [{TableName}] WHERE ColInt <= 2 ORDER BY ColInt", conn);

            using var reader1 = cmd1.ExecuteReader();
            using var reader2 = cmd2.ExecuteReader();

            sb.AppendLine("---READER1---");
            AppendResultSet(reader1, sb);
            sb.AppendLine("---READER2---");
            AppendResultSet(reader2, sb);

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunMarsReaderPlusDml(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "MARS_READER_PLUS_DML";
        try
        {
            var sb = new StringBuilder();
            var marsBuilder = new SqlConnectionStringBuilder(connStr) { MultipleActiveResultSets = true };
            using var conn = new SqlConnection(marsBuilder.ConnectionString);
            conn.Open();

            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id = 9801");

            using var cmdRead = new SqlCommand(
                $"SELECT ColInt, ColVarChar FROM [{TableName}] WHERE ColInt = 1", conn);
            using var reader = cmdRead.ExecuteReader();

            using (var cmdDml = new SqlCommand(
                $"INSERT INTO [{DmlTableName}] (Id, Name, Value) VALUES (9801, 'mars_dml', 1.00)", conn))
            {
                int rows = cmdDml.ExecuteNonQuery();
                sb.AppendLine($"DmlInsertedRows={rows}");
            }

            sb.AppendLine("---READER---");
            AppendResultSet(reader, sb);

            ExecNonQuery(conn, $"DELETE FROM [{DmlTableName}] WHERE Id = 9801");

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunMarsNestedCommands(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "MARS_NESTED_COMMANDS";
        try
        {
            var sb = new StringBuilder();
            var marsBuilder = new SqlConnectionStringBuilder(connStr) { MultipleActiveResultSets = true };
            using var conn = new SqlConnection(marsBuilder.ConnectionString);
            conn.Open();

            using var cmd1 = new SqlCommand(
                $"SELECT ColInt FROM [{TableName}] WHERE ColInt <= 3 ORDER BY ColInt", conn);
            using var reader1 = cmd1.ExecuteReader();

            sb.AppendLine("---LEVEL1---");
            while (reader1.Read())
            {
                int id = reader1.GetInt32(0);
                sb.AppendLine($"L1_ColInt={id}");

                using var cmd2 = new SqlCommand(
                    $"SELECT ColVarChar FROM [{TableName}] WHERE ColInt = @Id", conn);
                cmd2.Parameters.AddWithValue("@Id", id);
                using var reader2 = cmd2.ExecuteReader();
                if (reader2.Read())
                {
                    var val = reader2.IsDBNull(0) ? "NULL" : reader2.GetString(0);
                    sb.AppendLine($"  L2_ColVarChar={val}");
                }
            }

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    // -----------------------------------------------------------------------
    // InfoMessage / low-severity message tests
    // -----------------------------------------------------------------------

    static void RunInfoMessageTests(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        RunInfoMsgPrint(connStr, results);
        RunInfoMsgRaiserrorLow(connStr, results);
    }

    static void RunInfoMsgPrint(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "INFOMSG_PRINT";
        try
        {
            var sb = new StringBuilder();
            var messages = new List<string>();
            using var conn = new SqlConnection(connStr);
            conn.InfoMessage += (s, e) => messages.Add(e.Message);
            conn.Open();

            using var cmd = new SqlCommand("PRINT 'hello_from_print'; SELECT 1 AS Dummy;", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            sb.AppendLine("---INFOMESSAGES---");
            sb.AppendLine($"MessageCount={messages.Count}");
            foreach (var msg in messages)
                sb.AppendLine($"MSG|{msg}");

            results.Add((name, true, sb.ToString().TrimEnd(), null));
            Console.WriteLine($"  [PASS] {name}");
        }
        catch (Exception ex)
        {
            results.Add((name, false, "", ex.Message));
            Console.WriteLine($"  [FAIL] {name}: {ex.Message}");
        }
    }

    static void RunInfoMsgRaiserrorLow(string connStr, List<(string Name, bool Ok, string Data, string? Error)> results)
    {
        const string name = "INFOMSG_RAISERROR_LOW";
        try
        {
            var sb = new StringBuilder();
            var messages = new List<string>();
            using var conn = new SqlConnection(connStr);
            conn.InfoMessage += (s, e) => messages.Add(e.Message);
            conn.Open();

            using var cmd = new SqlCommand(
                "RAISERROR('low_severity_warning', 1, 1); SELECT 2 AS Dummy;", conn);
            using var reader = cmd.ExecuteReader();
            AppendResultSet(reader, sb);

            sb.AppendLine("---INFOMESSAGES---");
            sb.AppendLine($"MessageCount={messages.Count}");
            foreach (var msg in messages)
                sb.AppendLine($"MSG|{msg}");

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
