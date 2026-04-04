using Microsoft.Data.SqlClient;

namespace MaskingProxyTests;

public static class MaskingTestSetup
{
    public const string TestDbName = "MitmMaskingTestDb";
    public const string TableName = "MaskingTestTable";
    public const string AllTypesTableName = "AllTypesTestTable";
    public const string MaxTypesTableName = "MaxTypesTestTable";

    public const string DirectConnBase =
        "Server=127.0.0.1,1433;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true;Encrypt=true;";
    public const string ProxyConnBase =
        "Server=127.0.0.1,21433;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=true;Encrypt=false;";

    public static string DirectConn => DirectConnBase + $"Database={TestDbName};";
    public static string ProxyConn => ProxyConnBase + $"Database={TestDbName};";

    public static void SetupDatabase()
    {
        Console.WriteLine($"Setting up masking test database [{TestDbName}]...");

        using (var conn = new SqlConnection(DirectConnBase + "Database=master;"))
        {
            conn.Open();
            ExecNonQuery(conn, $@"
                IF DB_ID('{TestDbName}') IS NULL
                    CREATE DATABASE [{TestDbName}];");
        }
        Console.WriteLine("  Database created (or already exists).");

        using (var conn = new SqlConnection(DirectConn))
        {
            conn.Open();

            ExecNonQuery(conn, $@"
                IF OBJECT_ID('{TableName}', 'U') IS NOT NULL
                    DROP TABLE [{TableName}];");

            ExecNonQuery(conn, $@"
                CREATE TABLE [{TableName}] (
                    Id       INT NOT NULL PRIMARY KEY,
                    Name     NVARCHAR(100),
                    SSN      VARCHAR(20),
                    Email    NVARCHAR(200),
                    Age      INT,
                    Balance  DECIMAL(18,2)
                );");
            Console.WriteLine("  Table created.");

            ExecNonQuery(conn, $@"
                INSERT INTO [{TableName}] (Id, Name, SSN, Email, Age, Balance) VALUES
                (1, N'Alice Johnson',  '123-45-6789', N'alice@example.com',    30, 1500.75),
                (2, N'Bob Smith',      '987-65-4321', N'bob.smith@test.org',   45, 2300.00),
                (3, N'Charlie Brown',  '555-12-9876', N'charlie@domain.net',   28, 850.50),
                (4, N'Diana Prince',   '111-22-3333', N'diana.p@heroes.com',   35, 9999.99),
                (5, NULL,              NULL,           NULL,                    22, 100.00);");
            Console.WriteLine("  5 rows inserted (row 5 has NULLs for masked columns).");

            SetupAllTypesTable(conn);
            SetupMaxTypesTable(conn);
        }

        Console.WriteLine("Setup complete.");
    }

    static void SetupAllTypesTable(SqlConnection conn)
    {
        ExecNonQuery(conn, $@"
            IF OBJECT_ID('{AllTypesTableName}', 'U') IS NOT NULL
                DROP TABLE [{AllTypesTableName}];");

        ExecNonQuery(conn, $@"
            CREATE TABLE [{AllTypesTableName}] (
                Id              INT NOT NULL PRIMARY KEY,
                -- Alternate string types (masked via Name/SSN/Email column names)
                Name            NCHAR(100),
                SSN             CHAR(20),
                Email           NTEXT,
                -- Integers
                ColTinyInt      TINYINT,
                ColSmallInt     SMALLINT,
                ColBigInt       BIGINT,
                -- Bit
                ColBit          BIT,
                -- Float / Real
                ColFloat        FLOAT,
                ColReal         REAL,
                -- Money
                ColMoney        MONEY,
                ColSmallMoney   SMALLMONEY,
                -- Datetime family
                ColDate         DATE,
                ColTime         TIME,
                ColDateTime     DATETIME,
                ColSmallDateTime SMALLDATETIME,
                ColDateTime2    DATETIME2,
                ColDateTimeOffset DATETIMEOFFSET,
                -- Binary
                ColBinary       BINARY(16),
                ColVarBinary    VARBINARY(64),
                -- GUID
                ColGuid         UNIQUEIDENTIFIER,
                -- XML
                ColXml          XML,
                -- SQL_VARIANT
                ColVariant      SQL_VARIANT
            );");
        Console.WriteLine("  AllTypesTestTable created.");

        ExecNonQuery(conn, $@"
            INSERT INTO [{AllTypesTableName}] (
                Id, Name, SSN, Email,
                ColTinyInt, ColSmallInt, ColBigInt, ColBit,
                ColFloat, ColReal, ColMoney, ColSmallMoney,
                ColDate, ColTime, ColDateTime, ColSmallDateTime, ColDateTime2, ColDateTimeOffset,
                ColBinary, ColVarBinary, ColGuid, ColXml, ColVariant
            ) VALUES
            (1,
             N'Alice Johnson', '123-45-6789', N'alice@example.com',
             255, 32000, 9223372036854775807, 1,
             3.14159265358979, 2.71828, 922337203685477.5807, 214748.3647,
             '2025-06-15', '13:45:30.1234567', '2025-06-15 13:45:30.123', '2025-06-15 13:45:00',
             '2025-06-15 13:45:30.1234567', '2025-06-15 13:45:30.1234567 +05:30',
             0x0102030405060708090A0B0C0D0E0F10,
             0xDEADBEEFCAFE,
             'A1B2C3D4-E5F6-7890-ABCD-EF1234567890',
             N'<root><item id=""1"">Hello</item></root>',
             CAST(42 AS INT));");

        ExecNonQuery(conn, $@"
            INSERT INTO [{AllTypesTableName}] (
                Id, Name, SSN, Email,
                ColTinyInt, ColSmallInt, ColBigInt, ColBit,
                ColFloat, ColReal, ColMoney, ColSmallMoney,
                ColDate, ColTime, ColDateTime, ColSmallDateTime, ColDateTime2, ColDateTimeOffset,
                ColBinary, ColVarBinary, ColGuid, ColXml, ColVariant
            ) VALUES
            (2,
             N'Bob Smith', '987-65-4321', N'bob.smith@test.org',
             0, -32000, -9223372036854775808, 0,
             -1.23e10, -0.5, -922337203685477.5808, -214748.3648,
             '1999-01-01', '00:00:00.0000000', '1999-01-01 00:00:00.000', '1999-01-01 00:00:00',
             '1999-01-01 00:00:00.0000000', '1999-01-01 00:00:00.0000000 -08:00',
             0x00000000000000000000000000000000,
             0xFF,
             '00000000-0000-0000-0000-000000000000',
             N'<empty/>',
             CAST(N'variant text' AS NVARCHAR(50)));");

        ExecNonQuery(conn, $@"
            INSERT INTO [{AllTypesTableName}] (
                Id, Name, SSN, Email,
                ColTinyInt, ColSmallInt, ColBigInt, ColBit,
                ColFloat, ColReal, ColMoney, ColSmallMoney,
                ColDate, ColTime, ColDateTime, ColSmallDateTime, ColDateTime2, ColDateTimeOffset,
                ColBinary, ColVarBinary, ColGuid, ColXml, ColVariant
            ) VALUES
            (3,
             NULL, NULL, NULL,
             NULL, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL);");
        Console.WriteLine("  AllTypesTestTable: 3 rows inserted (row 3 has NULLs).");
    }

    static void SetupMaxTypesTable(SqlConnection conn)
    {
        ExecNonQuery(conn, $@"
            IF OBJECT_ID('{MaxTypesTableName}', 'U') IS NOT NULL
                DROP TABLE [{MaxTypesTableName}];");

        ExecNonQuery(conn, $@"
            CREATE TABLE [{MaxTypesTableName}] (
                Id           INT NOT NULL PRIMARY KEY,
                Name         NVARCHAR(MAX),
                SSN          VARCHAR(MAX),
                Email        NVARCHAR(MAX),
                ColVarBinMax VARBINARY(MAX),
                ColSmallName NVARCHAR(50)
            );");
        Console.WriteLine("  MaxTypesTestTable created.");

        ExecNonQuery(conn, $@"
            INSERT INTO [{MaxTypesTableName}] (Id, Name, SSN, Email, ColVarBinMax, ColSmallName)
            VALUES
            (1, N'Alice Johnson', '123-45-6789', N'alice@example.com',
             0xDEADBEEFCAFE, N'Alice');");

        ExecNonQuery(conn, $@"
            INSERT INTO [{MaxTypesTableName}] (Id, Name, SSN, Email, ColVarBinMax, ColSmallName)
            VALUES
            (2, NULL, NULL, NULL, NULL, N'NullRow');");

        ExecNonQuery(conn, $@"
            INSERT INTO [{MaxTypesTableName}] (Id, Name, SSN, Email, ColVarBinMax, ColSmallName)
            VALUES
            (3, N'{new string('X', 9000)}', '987-65-4321', N'bob.smith@test.org',
             0x{string.Concat(Enumerable.Repeat("FF", 9000))}, N'LongRow');");

        Console.WriteLine("  MaxTypesTestTable: 3 rows inserted (row 2 NULLs, row 3 large PLP).");
    }

    public static void TeardownDatabase()
    {
        Console.WriteLine($"Tearing down masking test database [{TestDbName}]...");
        using var conn = new SqlConnection(DirectConnBase + "Database=master;");
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
