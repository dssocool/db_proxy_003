using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace SqlProxyTests;

public static class TestSetup
{
    public const string TestDbName = "MitmSqlProxyTestDb";
    public const string TableName = "AllTypesTable";
    public const string DmlTableName = "DmlTestTable";
    public const string DmlAuditTableName = "DmlAuditLog";
    public const string MergeSourceTableName = "MergeSourceTable";
    public const string TxnTableName = "TxnTestTable";
    public const string ErrParentTableName = "ErrParentTable";
    public const string ErrChildTableName = "ErrChildTable";
    public const string BulkCopyTableName = "BulkCopyTarget";
    public const string AdapterTableName = "AdapterTestTable";

    static TestSetup()
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        var proxySection = config.GetSection("Proxy");
        int listenPort = int.Parse(proxySection["ListenPort"] ?? "1433");
        string sqlUsername = proxySection["SqlUsername"] ?? "proxyuser";
        string sqlPassword = proxySection["SqlPassword"] ?? "proxypassword";
        string backendConnStr = proxySection["BackendConnectionString"]
            ?? throw new InvalidOperationException("BackendConnectionString not found in appsettings.json");

        var backend = new SqlConnectionStringBuilder(backendConnStr);
        DirectConnBase = new SqlConnectionStringBuilder
        {
            DataSource = backend.DataSource,
            UserID = backend.UserID,
            Password = backend.Password,
            TrustServerCertificate = true,
            Encrypt = backend.Encrypt,
        }.ConnectionString;

        ProxyConnBase = new SqlConnectionStringBuilder
        {
            DataSource = $"127.0.0.1,{listenPort}",
            UserID = sqlUsername,
            Password = sqlPassword,
            TrustServerCertificate = true,
            Encrypt = false,
        }.ConnectionString;
    }

    public static readonly string DirectConnBase;
    public static readonly string ProxyConnBase;

    public static string DirectConn => new SqlConnectionStringBuilder(DirectConnBase)
        { InitialCatalog = TestDbName }.ConnectionString;
    public static string ProxyConn => new SqlConnectionStringBuilder(ProxyConnBase)
        { InitialCatalog = TestDbName }.ConnectionString;

    public static void SetupDatabase()
    {
        Console.WriteLine($"Setting up test database [{TestDbName}]...");

        using (var conn = new SqlConnection(
            new SqlConnectionStringBuilder(DirectConnBase) { InitialCatalog = "master" }.ConnectionString))
        {
            conn.Open();
            ExecNonQuery(conn, $@"
                IF DB_ID('{TestDbName}') IS NULL
                    CREATE DATABASE [{TestDbName}];");
        }
        Console.WriteLine("  Database created (or already exists).");

        using (var conn = new SqlConnection(
            new SqlConnectionStringBuilder(DirectConnBase) { InitialCatalog = TestDbName }.ConnectionString))
        {
            conn.Open();

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{TableName}', 'U') IS NOT NULL
                    DROP TABLE [{TableName}];");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{TableName}] (
                    -- Exact numeric
                    ColBigInt           bigint,
                    ColInt              int NOT NULL PRIMARY KEY,
                    ColSmallInt         smallint,
                    ColTinyInt          tinyint,
                    ColBit              bit,
                    ColDecimal          decimal(18,4),
                    ColNumeric          numeric(10,2),
                    ColMoney            money,
                    ColSmallMoney       smallmoney,

                    -- Approximate numeric
                    ColFloat            float,
                    ColReal             real,

                    -- Date / time
                    ColDate             date,
                    ColDateTime         datetime,
                    ColDateTime2        datetime2(7),
                    ColDateTimeOffset   datetimeoffset,
                    ColSmallDateTime    smalldatetime,
                    ColTime             time,

                    -- Character strings
                    ColChar             char(10),
                    ColVarChar          varchar(100),
                    ColVarCharMax       varchar(max),
                    ColText             text,

                    -- Unicode strings
                    ColNChar            nchar(10),
                    ColNVarChar         nvarchar(100),
                    ColNVarCharMax      nvarchar(max),
                    ColNText            ntext,

                    -- Binary
                    ColBinary           binary(8),
                    ColVarBinary        varbinary(100),
                    ColVarBinaryMax     varbinary(max),
                    ColImage            image,

                    -- Other
                    ColUniqueIdentifier uniqueidentifier,
                    ColXml              xml,
                    ColSqlVariant       sql_variant
                );");
            Console.WriteLine("  Table created.");

            ExecNonQuery(conn, $@"
                INSERT INTO [{TableName}] VALUES (
                    100000000000, 1, 100, 10, 1,
                    12345.6789, 9876.54, 19.99, 9.99,
                    3.14159265358979, 2.718,
                    '2024-01-15', '2024-01-15 10:30:00', '2024-01-15 10:30:00.1234567',
                    '2024-01-15 10:30:00.1234567 -05:00', '2024-01-15 10:30:00', '10:30:00.1234567',
                    'hello     ', 'hello world', 'long varchar max value here', 'text value one',
                    N'unicode   ', N'hello unicode', N'long nvarchar max value', N'ntext value one',
                    0x0102030405060708, 0xDEADBEEF, 0xCAFEBABE, 0xFF,
                    'A0000000-0000-0000-0000-000000000001',
                    '<root><item>first</item></root>',
                    CAST(42 AS INT)
                );

                INSERT INTO [{TableName}] VALUES (
                    -200000000000, 2, -200, 20, 0,
                    -9999.1234, -1234.56, -500.50, -99.99,
                    -1.23456789, -0.5,
                    '2023-06-30', '2023-06-30 23:59:59', '2023-06-30 23:59:59.9999999',
                    '2023-06-30 23:59:59.9999999 +05:30', '2023-06-30 23:59:00', '23:59:59.9999999',
                    'world     ', 'test string', 'another long varchar max', 'text value two',
                    N'unicode2  ', N'test nvarchar', N'another nvarchar max', N'ntext value two',
                    0x08070605040302FF, 0xBAADF00D, 0x0000000000, 0x0102,
                    'B0000000-0000-0000-0000-000000000002',
                    '<root><item>second</item><item>extra</item></root>',
                    CAST('variant string' AS VARCHAR(50))
                );

                INSERT INTO [{TableName}] VALUES (
                    0, 3, 0, 0, 1,
                    0.0000, 0.00, 0.00, 0.00,
                    0.0, 0.0,
                    '2000-01-01', '2000-01-01 00:00:00', '2000-01-01 00:00:00.0000000',
                    '2000-01-01 00:00:00.0000000 +00:00', '2000-01-01 00:00:00', '00:00:00.0000000',
                    'zero      ', 'zero value', 'zero max', 'text zero',
                    N'zero      ', N'zero nvarchar', N'zero nvarchar max', N'ntext zero',
                    0x0000000000000000, 0x00, 0x00, 0x00,
                    '00000000-0000-0000-0000-000000000000',
                    '<root/>',
                    CAST(0 AS BIT)
                );

                INSERT INTO [{TableName}] (
                    ColBigInt, ColInt, ColSmallInt, ColTinyInt, ColBit,
                    ColDecimal, ColNumeric, ColMoney, ColSmallMoney,
                    ColFloat, ColReal,
                    ColDate, ColDateTime, ColDateTime2,
                    ColDateTimeOffset, ColSmallDateTime, ColTime,
                    ColChar, ColVarChar, ColVarCharMax, ColText,
                    ColNChar, ColNVarChar, ColNVarCharMax, ColNText,
                    ColBinary, ColVarBinary, ColVarBinaryMax, ColImage,
                    ColUniqueIdentifier, ColXml, ColSqlVariant
                ) VALUES (
                    NULL, 4, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL
                );

                INSERT INTO [{TableName}] VALUES (
                    9223372036854775807, 5, 32767, 255, 1,
                    99999999999999.9999, 99999999.99, 922337203685477.5807, 214748.3647,
                    1.7E+308, 3.4E+38,
                    '9999-12-31', '2079-06-06 23:59:00', '9999-12-31 23:59:59.9999999',
                    '9999-12-31 23:59:59.9999999 +14:00', '2079-06-06 23:59:00', '23:59:59.9999999',
                    'maxvals   ', 'max boundary test', 'max varchar max boundary', 'text max boundary',
                    N'maxvals   ', N'max nvarchar boundary', N'max nvarchar max boundary', N'ntext max boundary',
                    0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFF, 0xFFFF,
                    'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF',
                    '<root><item attr=""x"">boundary</item></root>',
                    CAST(3.14 AS FLOAT)
                );");
            Console.WriteLine("  5 rows inserted.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_GetById', 'P') IS NOT NULL DROP PROCEDURE usp_GetById;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_GetById @Id INT
                AS
                BEGIN
                    SELECT * FROM [{TableName}] WHERE ColInt = @Id;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_GetByRange', 'P') IS NOT NULL DROP PROCEDURE usp_GetByRange;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_GetByRange @MinId INT, @MaxId INT
                AS
                BEGIN
                    SELECT ColInt, ColVarChar, ColBigInt, ColDecimal FROM [{TableName}]
                    WHERE ColInt BETWEEN @MinId AND @MaxId ORDER BY ColInt;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_GetCount', 'P') IS NOT NULL DROP PROCEDURE usp_GetCount;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_GetCount
                AS
                BEGIN
                    SELECT COUNT(*) AS Cnt FROM [{TableName}];
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_GetNameById', 'P') IS NOT NULL DROP PROCEDURE usp_GetNameById;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_GetNameById @Id INT, @Name VARCHAR(100) OUTPUT
                AS
                BEGIN
                    SELECT @Name = ColVarChar FROM [{TableName}] WHERE ColInt = @Id;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_MultiResult', 'P') IS NOT NULL DROP PROCEDURE usp_MultiResult;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_MultiResult @Id INT
                AS
                BEGIN
                    SELECT ColInt, ColVarChar FROM [{TableName}] WHERE ColInt = @Id;
                    SELECT COUNT(*) AS Total FROM [{TableName}];
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_WithDefaults', 'P') IS NOT NULL DROP PROCEDURE usp_WithDefaults;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_WithDefaults @Id INT, @Label VARCHAR(50) = 'default'
                AS
                BEGIN
                    SELECT ColInt, ColVarChar, @Label AS Label FROM [{TableName}] WHERE ColInt = @Id;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_ReturnStatus', 'P') IS NOT NULL DROP PROCEDURE usp_ReturnStatus;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_ReturnStatus @Id INT
                AS
                BEGIN
                    IF EXISTS (SELECT 1 FROM [{TableName}] WHERE ColInt = @Id)
                        RETURN 1;
                    RETURN 0;
                END");

            Console.WriteLine("  Stored procedures created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{DmlAuditTableName}', 'U') IS NOT NULL DROP TABLE [{DmlAuditTableName}];
                IF OBJECT_ID('{DmlTableName}', 'U') IS NOT NULL DROP TABLE [{DmlTableName}];
                IF OBJECT_ID('{MergeSourceTableName}', 'U') IS NOT NULL DROP TABLE [{MergeSourceTableName}];");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{DmlTableName}] (
                    Id    INT NOT NULL PRIMARY KEY,
                    Name  VARCHAR(100),
                    Value DECIMAL(18,2)
                );");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{DmlAuditTableName}] (
                    LogId       INT IDENTITY(1,1) PRIMARY KEY,
                    AuditAction VARCHAR(10),
                    AuditId     INT,
                    AuditTs     DATETIME2 DEFAULT SYSUTCDATETIME()
                );");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{MergeSourceTableName}] (
                    Id    INT NOT NULL PRIMARY KEY,
                    Name  VARCHAR(100),
                    Value DECIMAL(18,2)
                );");
            Console.WriteLine("  DML tables created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('trg_DmlInsert', 'TR') IS NOT NULL DROP TRIGGER trg_DmlInsert;");
            ExecNonQuery(conn, $@"
                CREATE TRIGGER trg_DmlInsert ON [{DmlTableName}] AFTER INSERT
                AS
                BEGIN
                    INSERT INTO [{DmlAuditTableName}] (AuditAction, AuditId)
                    SELECT 'INSERT', Id FROM inserted;
                END");
            Console.WriteLine("  DML trigger created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_DmlInsertOutput', 'P') IS NOT NULL DROP PROCEDURE usp_DmlInsertOutput;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_DmlInsertOutput
                    @Id INT, @Name VARCHAR(100), @Val DECIMAL(18,2), @RowsAffected INT OUTPUT
                AS
                BEGIN
                    DECLARE @out TABLE (Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                    INSERT INTO [{DmlTableName}] (Id, Name, Value)
                    OUTPUT inserted.Id, inserted.Name, inserted.Value INTO @out
                    VALUES (@Id, @Name, @Val);
                    SET @RowsAffected = @@ROWCOUNT;
                    SELECT * FROM @out;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_DmlUpdateOutput', 'P') IS NOT NULL DROP PROCEDURE usp_DmlUpdateOutput;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_DmlUpdateOutput
                    @Id INT, @NewName VARCHAR(100), @NewVal DECIMAL(18,2), @RowsAffected INT OUTPUT
                AS
                BEGIN
                    DECLARE @out TABLE (Id INT, OldName VARCHAR(100), OldValue DECIMAL(18,2), NewName VARCHAR(100), NewValue DECIMAL(18,2));
                    UPDATE [{DmlTableName}]
                    SET Name = @NewName, Value = @NewVal
                    OUTPUT deleted.Id, deleted.Name, deleted.Value,
                           inserted.Name, inserted.Value INTO @out
                    WHERE Id = @Id;
                    SET @RowsAffected = @@ROWCOUNT;
                    SELECT * FROM @out;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_DmlDeleteOutput', 'P') IS NOT NULL DROP PROCEDURE usp_DmlDeleteOutput;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_DmlDeleteOutput
                    @Id INT, @RowsAffected INT OUTPUT
                AS
                BEGIN
                    DECLARE @out TABLE (Id INT, Name VARCHAR(100), Value DECIMAL(18,2));
                    DELETE FROM [{DmlTableName}]
                    OUTPUT deleted.Id, deleted.Name, deleted.Value INTO @out
                    WHERE Id = @Id;
                    SET @RowsAffected = @@ROWCOUNT;
                    SELECT * FROM @out;
                END");
            Console.WriteLine("  DML stored procedures created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{TxnTableName}', 'U') IS NOT NULL DROP TABLE [{TxnTableName}];");
            ExecNonQuery(conn, $@"
                CREATE TABLE [{TxnTableName}] (
                    Id  INT NOT NULL PRIMARY KEY,
                    Val VARCHAR(200)
                );");
            Console.WriteLine("  Transaction test table created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{ErrChildTableName}', 'U') IS NOT NULL DROP TABLE [{ErrChildTableName}];
                IF OBJECT_ID('{ErrParentTableName}', 'U') IS NOT NULL DROP TABLE [{ErrParentTableName}];");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{ErrParentTableName}] (
                    Id INT NOT NULL PRIMARY KEY
                );
                INSERT INTO [{ErrParentTableName}] (Id) VALUES (1), (2), (3);");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{ErrChildTableName}] (
                    Id       INT NOT NULL PRIMARY KEY,
                    ParentId INT NOT NULL REFERENCES [{ErrParentTableName}](Id)
                );");
            Console.WriteLine("  Error propagation FK tables created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_TvpSelect', 'P') IS NOT NULL DROP PROCEDURE usp_TvpSelect;");
            ExecNonQuery(conn, @"
                IF TYPE_ID('dbo.IdNameType') IS NOT NULL DROP TYPE dbo.IdNameType;");
            ExecNonQuery(conn, @"
                CREATE TYPE dbo.IdNameType AS TABLE (Id INT, Name VARCHAR(100));");
            ExecNonQuery(conn, @"
                CREATE PROCEDURE usp_TvpSelect @Items dbo.IdNameType READONLY
                AS
                BEGIN
                    SELECT Id, Name FROM @Items ORDER BY Id;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_EchoVarBinary', 'P') IS NOT NULL DROP PROCEDURE usp_EchoVarBinary;");
            ExecNonQuery(conn, @"
                CREATE PROCEDURE usp_EchoVarBinary @Data VARBINARY(MAX), @DataLen INT OUTPUT
                AS
                BEGIN
                    SET @DataLen = DATALENGTH(@Data);
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_EchoNVarChar', 'P') IS NOT NULL DROP PROCEDURE usp_EchoNVarChar;");
            ExecNonQuery(conn, @"
                CREATE PROCEDURE usp_EchoNVarChar @Data NVARCHAR(MAX), @DataLen INT OUTPUT
                AS
                BEGIN
                    SET @DataLen = LEN(@Data);
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_EchoDecimal', 'P') IS NOT NULL DROP PROCEDURE usp_EchoDecimal;");
            ExecNonQuery(conn, @"
                CREATE PROCEDURE usp_EchoDecimal @Val DECIMAL(38,18), @Result DECIMAL(38,18) OUTPUT
                AS
                BEGIN
                    SET @Result = @Val;
                END");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('usp_MixedOutputReturnResultSet', 'P') IS NOT NULL DROP PROCEDURE usp_MixedOutputReturnResultSet;");
            ExecNonQuery(conn, $@"
                CREATE PROCEDURE usp_MixedOutputReturnResultSet
                    @Id INT, @OutName VARCHAR(100) OUTPUT
                AS
                BEGIN
                    SELECT @OutName = ColVarChar FROM [{TableName}] WHERE ColInt = @Id;
                    SELECT ColInt, ColVarChar, ColDecimal FROM [{TableName}] WHERE ColInt = @Id;
                    SELECT COUNT(*) AS Total FROM [{TableName}];
                    IF @OutName IS NOT NULL RETURN 1;
                    RETURN 0;
                END");
            Console.WriteLine("  Advanced RPC objects created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{BulkCopyTableName}', 'U') IS NOT NULL DROP TABLE [{BulkCopyTableName}];");
            ExecNonQuery(conn, $@"
                CREATE TABLE [{BulkCopyTableName}] (
                    Id    INT NOT NULL PRIMARY KEY,
                    Name  VARCHAR(100),
                    Value DECIMAL(18,2)
                );");
            Console.WriteLine("  BulkCopy target table created.");

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{AdapterTableName}', 'U') IS NOT NULL DROP TABLE [{AdapterTableName}];");
            ExecNonQuery(conn, $@"
                CREATE TABLE [{AdapterTableName}] (
                    Id  INT NOT NULL PRIMARY KEY,
                    Val VARCHAR(100)
                );");
            Console.WriteLine("  Adapter test table created.");
        }

        Console.WriteLine("Setup complete.");
    }

    public static void TeardownDatabase()
    {
        Console.WriteLine($"Tearing down test database [{TestDbName}]...");
        using var conn = new SqlConnection(
            new SqlConnectionStringBuilder(DirectConnBase) { InitialCatalog = "master" }.ConnectionString);
        conn.Open();
        ExecNonQuery(conn, $@"
            IF DB_ID('{TestDbName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{TestDbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{TestDbName}];
            END");
        Console.WriteLine("Done.");
    }

    public static void ExecNonQuery(SqlConnection conn, string sql)
    {
        using var cmd = new SqlCommand(sql, conn);
        cmd.ExecuteNonQuery();
    }
}
