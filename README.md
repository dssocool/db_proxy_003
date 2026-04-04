# DbProxy

A small **TDS (Tabular Data Stream) terminating proxy** for SQL Server. Clients connect to this process with SQL authentication; the proxy validates the username and password, then opens a **backend** connection using `Microsoft.Data.SqlClient` (for example Azure SQL with **Microsoft Entra** / `Active Directory Default`).

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download/dotnet/9.0)

Check your install:

```bash
dotnet --version
```

## Configuration

Edit [`src/DbProxy/appsettings.json`](src/DbProxy/appsettings.json). The `Proxy` section is required:

| Setting | Purpose |
|--------|---------|
| `ListenPort` | TCP port the proxy listens on (default `1433`). |
| `SqlUsername` / `SqlPassword` | Credentials **clients must use** when connecting to the proxy (username is case-insensitive; password is case-sensitive). |
| `BackendConnectionString` | Connection string used **from the proxy to the real database** (SQL auth, Entra ID, etc.). Must be non-empty or the app exits with an error. |

The sample file uses placeholder values. Replace `BackendConnectionString` with a valid connection string for your environment, and set client-facing `SqlUsername` / `SqlPassword` as needed.

## Run

From the **repository root**:

```bash
dotnet restore
dotnet run --project src/DbProxy/DbProxy.csproj
```

Or from the project directory:

```bash
cd src/DbProxy
dotnet run
```

**Release build:**

```bash
dotnet build DbProxy.sln -c Release
dotnet run --project src/DbProxy/DbProxy.csproj -c Release
```

Stop the process with **Ctrl+C**.

## Connect as a client

Point your SQL client at `localhost` (or the host running the proxy) and the configured `ListenPort`. Use the SQL login matching `SqlUsername` / `SqlPassword` in `appsettings.json`. The proxy handles TDS pre-login and login; after a successful login it uses `BackendConnectionString` for queries.

## Notes

- **Port 1433**: On Windows, binding to low ports may require elevation or a URL reservation, depending on policy. If bind fails, try a higher port (for example `14333`) in `ListenPort`.
- **Secrets**: Prefer environment-specific config or user secrets for production; do not commit real passwords or connection strings.
- **Logging**: The app uses console logging at debug level (see `Program.cs`).
