# SQL Data Comparer

Compare row-level data between two **SQL Server**, **PostgreSQL**, or **MySQL** databases, or between **folders of `INSERT` scripts**. Use the **WPF** desktop app, the **CLI**, or a bundled **[MCP](https://modelcontextprotocol.io/) server** so tools like Cursor and Claude Desktop can enumerate tables, inspect rows and FKs, diff tables, and apply syncs. Reports: JSON, HTML, CSV, JUnit-style XML.

**This repo includes a first-party MCP host** (`SqlDataCompare.Mcp`)—no separate package. The CLI supports **compare-only** (default) or **`--copy-differences`** to push inserts/updates/deletes to the destination after a successful compare.

Repository: [github.com/felenko/SQL-Data-Comparer](https://github.com/felenko/SQL-Data-Comparer)

## Solution layout

| Project | Role |
|---------|------|
| **SqlDataCompare.Core** | Compare engine, sync, schema, project model, MCP helpers |
| **SqlDataCompare.Wpf** | Windows UI (`net8.0-windows`) |
| **SqlDataCompare.Cli** | Command-line compare and optional sync |
| **SqlDataCompare.Mcp** | MCP stdio server exposing Core as tools |
| **SqlDataCompare.Tests** | Unit tests |
| **SqlDataCompare.Mcp.IntegrationTests** | MCP-related checks (see below) |

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- **WPF UI**: Windows (`net8.0-windows`)

## Build

```bash
dotnet build SqlDataCompare.sln
```

## Run tests

```bash
dotnet test SqlDataCompare.Tests/SqlDataCompare.Tests.csproj
```

**MCP integration tests** (`SqlDataCompare.Mcp.IntegrationTests`):

```bash
dotnet test SqlDataCompare.Mcp.IntegrationTests/SqlDataCompare.Mcp.IntegrationTests.csproj
```

- Always runs **source-level** checks that `SqlDataCompareMcpTools.cs` still defines five `[McpServerTool]` methods (no dependency on the MCP exe, so this project builds even while `SqlDataCompare.Mcp` is running).
- **SQL-backed** tests run only when you set **`SDC_MCP_INTEGRATION_SQL`** to a SQL Server **ADO.NET connection string** (any database with at least one table that has a resolvable primary key). If unset, those tests are **skipped** so CI stays green without secrets.

Example (PowerShell, session only):

```powershell
$env:SDC_MCP_INTEGRATION_SQL = 'Data Source=localhost\sqlexpress;Initial Catalog=YourDb;User ID=...;Password=...;Encrypt=True;Trust Server Certificate=True'
dotnet test SqlDataCompare.Mcp.IntegrationTests/SqlDataCompare.Mcp.IntegrationTests.csproj
```

## MCP server (stdio)

The server uses the [C# MCP SDK](https://github.com/modelcontextprotocol/csharp-sdk) (`ModelContextProtocol` on NuGet). Run:

```bash
dotnet run --project SqlDataCompare.Mcp/SqlDataCompare.Mcp.csproj
```

**Tools** (exact names may be prefixed by the SDK; check your client’s tool list):

| Tool | Purpose |
|------|--------|
| **SqlEnumerateTables** | List tables (`schema`, `name`, `display`) for a connection. |
| **SqlGetTableDataWithRelations** | Read up to N rows (all columns) plus **incoming/outgoing foreign keys** for that table. |
| **SqlCompareTwoTables** | Compare one source table to one destination; JSON includes value diffs and samples. |
| **SqlCopySelectedRowsToDestination** | Sync **specific rows** (JSON array of key objects); set `rowOperationKind` and `insertMissing` / `updateChanged` / `deleteExtra`. **Writes destination only.** |
| **SqlCopyProjectDifferencesToDestination** | Sync **all** differences for every table from a **full compare project JSON** (same shape as a saved `.json` project, including polymorphic `"kind": "database"` on endpoints). Full merge per table—not limited to compare sample rows. **Writes destination only.** |

**Cursor example** (adjust the path):

```json
"sql-data-compare": {
  "command": "dotnet",
  "args": ["run", "--project", "C:/Source/Claude/SqlDataCompare/SqlDataCompare.Mcp/SqlDataCompare.Mcp.csproj"]
}
```

Treat connection strings and project JSON with embedded secrets as **sensitive**. Prefer environment variables or client-injected config, not committed files.

## Desktop app (Windows)

```bash
dotnet run --project SqlDataCompare.Wpf/SqlDataCompare.Wpf.csproj
```

Load or create a compare project, run compare, review results, then optionally sync selected tables to the destination database.

## Command line

```bash
dotnet run --project SqlDataCompare.Cli/SqlDataCompare.Cli.csproj -- --project path/to/project.json
```

Use **`--help`** or **`-h`** for the built-in usage text.

**Compare-only (default)** — Reports differences; does not modify any database.

**`--copy-differences`** — After compare succeeds (no per-table **Error**), runs the same style of sync as the app: **insert** missing rows, **update** changed values, and optionally **delete** extra rows on the destination. Applies to **all** tables in the project worklist (not row-by-row selection). Requires a **database** destination in the project. If compare reports **all tables identical**, sync is **skipped** (no full scan for writes).

**Options**

| Option | Description |
|--------|-------------|
| `--project <path>` | Compare project JSON (required). |
| `--report <path>` | Full compare results as JSON. |
| `--export-html <path>` | HTML report. |
| `--export-csv <path>` | CSV summary. |
| `--export-junit <path>` | JUnit-style XML. |
| `--copy-differences` | Sync to destination after successful compare (see above). |
| `--sync-report <path>` | Write sync results JSON (requires `--copy-differences`). |
| `--sync-delete-extra` | Allow **DELETE** on destination for keys not on source (default: off). |
| `--sync-no-insert-missing` | Do not INSERT missing rows. |
| `--sync-no-update-changed` | Do not UPDATE changed rows. |
| `--disable-fk-checks` | Temporarily relax FK checks on the destination during sync. |
| `--verbosity` / `-v` | e.g. `Information`, `Debug`, `Trace`. |

**Exit codes**

| Code | Meaning |
|------|---------|
| 0 | Compare succeeded and, if `--copy-differences` was used, sync finished with no sync errors (or sync skipped because everything was identical). |
| 1 | Failure: bad project, connection error, compare error on a table, or sync error. |
| 2 | Compare-only: at least one table differs (not used when you pass `--copy-differences` and sync completes successfully). |

**Example — compare with reports**

```bash
dotnet run --project SqlDataCompare.Cli/SqlDataCompare.Cli.csproj -- ^
  --project MyProject.json ^
  --report out/compare.json ^
  --export-html out/report.html ^
  -v Information
```

**Example — compare then copy differences**

```bash
dotnet run --project SqlDataCompare.Cli/SqlDataCompare.Cli.csproj -- ^
  --project MyProject.json ^
  --copy-differences ^
  --sync-report out/sync.json ^
  -v Information
```

On PowerShell, use a backtick `` ` `` for line continuation instead of `^`.

## Project file (JSON)

Projects describe endpoints (`database` or `insert` folder), provider (`sqlserver`, `postgresql`, `mysql`), connection strings or paths, compare options, optional `tablesToCompare`, and `tableOverrides`.

Copy the sample and edit for your environment:

```bash
copy VisualCasino.example.json MyProject.json
```

**Do not commit real connection strings.** Keep secrets local; this repo’s `.gitignore` excludes `VisualCasino.json`. Use placeholders or CI-specific files.

See `samples/folder-to-folder.sample.json` for folder-based endpoints.

## Compare performance (troubleshooting)

Tables run in **schema/name order**. Slow stretches usually mean the **next** table is large or remote.

At **Information** verbosity, logs include per-table **`Loaded … in N ms`** and **`merge M ms`**. Source and destination row reads run **in parallel**; merge skips redundant sorting when SQL already returns rows ordered by key.

For heavy tables: use **`tableOverrides`** with **`maxRows`** for a sampled compare, or restrict **`tablesToCompare`**.

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE).
