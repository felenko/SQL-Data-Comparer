# MCP SQL Data Comparer

Compare row-level data between two **SQL Server**, **PostgreSQL**, or **MySQL** databases, or between **folders of `INSERT` scripts**, from a **WPF desktop app** or a **command-line** runner. **This implementation ships with a built-in [MCP](https://modelcontextprotocol.io/) (Model Context Protocol) server**—wire it into Cursor, Claude Desktop, or any MCP-capable client to enumerate schemas, read related rows, diff tables, and run targeted syncs from natural-language workflows. Optional reporting to JSON, HTML, CSV, and JUnit-style XML.

Repository: [github.com/felenko/SQL-Data-Comparer](https://github.com/felenko/SQL-Data-Comparer)

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

## MCP server (included; stdio)

**The MCP server is part of this repository** (`SqlDataCompare.Mcp`)—not a separate download. It exposes compare and sync capabilities over [Model Context Protocol](https://modelcontextprotocol.io/) stdio using the official [C# MCP SDK](https://github.com/modelcontextprotocol/csharp-sdk) (`ModelContextProtocol` on NuGet). Run:

```bash
dotnet run --project SqlDataCompare.Mcp/SqlDataCompare.Mcp.csproj
```

**Tools (names are prefixed by the SDK; see your client’s tool list):**

| Tool | Purpose |
|------|--------|
| **SqlEnumerateTables** | List tables (`schema`, `name`, `display`) for a connection. |
| **SqlGetTableDataWithRelations** | Read up to N rows (all columns) plus **incoming/outgoing foreign keys** involving that table. |
| **SqlCompareTwoTables** | Compare one source table to one destination table; JSON includes value diffs and samples. |
| **SqlCopySelectedRowsToDestination** | Sync **specific rows** (by key JSON array) to the destination; set `rowOperationKind` and `insertMissing` / `updateChanged` / `deleteExtra` to match the operation. **Writes to the destination only.** |

**Cursor example** (adjust the path):

```json
"sql-data-compare": {
  "command": "dotnet",
  "args": ["run", "--project", "C:/Source/Claude/SqlDataCompare/SqlDataCompare.Mcp/SqlDataCompare.Mcp.csproj"]
}
```

Connection strings are passed as tool arguments — treat them as secrets. Prefer env vars or a local config your client injects, not committed files.

## Desktop app (Windows)

```bash
dotnet run --project SqlDataCompare.Wpf/SqlDataCompare.Wpf.csproj
```

Load or create a compare project JSON, configure source and destination, run compare (and sync when you choose to apply changes).

## Command line (compare only)

The CLI loads a project file and runs a full compare. It does **not** sync data.

```bash
dotnet run --project SqlDataCompare.Cli/SqlDataCompare.Cli.csproj -- --project path/to/project.json
```

**Options**

| Option | Description |
|--------|-------------|
| `--project <path>` | Path to the compare project JSON (required). |
| `--report <path>` | Write full results as JSON. |
| `--export-html <path>` | HTML report. |
| `--export-csv <path>` | CSV summary. |
| `--export-junit <path>` | JUnit-style XML. |
| `--verbosity` / `-v` | e.g. `Information`, `Debug`, `Trace`. |

**Exit codes**

| Code | Meaning |
|------|---------|
| 0 | No differences (or only skipped tables). |
| 1 | Error (e.g. connection or invalid project). |
| 2 | At least one table differs. |

Example with reports:

```bash
dotnet run --project SqlDataCompare.Cli/SqlDataCompare.Cli.csproj -- ^
  --project MyProject.json ^
  --report out/compare.json ^
  --export-html out/report.html ^
  -v Information
```

(On PowerShell, use backtick `` ` `` for line continuation instead of `^`.)

## Project file (JSON)

Projects describe endpoints (`database` or `insert` folder), provider (`sqlserver`, `postgresql`, `mysql`), connection strings or paths, compare options, optional table list, and overrides.

Copy the sample and edit for your environment:

```bash
copy VisualCasino.example.json MyProject.json
```

**Do not commit real connection strings.** `VisualCasino.json` (or any file with secrets) should stay local; this repo’s `.gitignore` excludes `VisualCasino.json`. Use placeholders or environment-specific copies in CI.

See also `samples/folder-to-folder.sample.json` for folder-based endpoints.

## Compare performance (troubleshooting)

Tables are processed in **schema/name order**. If compare feels slow “after” a given table (e.g. `TapeColours`), the time is almost always spent on **the next table(s)** in that order—often a **large row count** or a **remote** source.

Logs at **Information** show per-table **`Loaded … in N ms`** (network / SQL) and **`merge M ms`** (CPU). A huge **merge** time previously came from **re-sorting** every row list in memory even though SQL already returned `ORDER BY` keys; the engine now **skips that sort** when order already matches. **Source and destination** row reads for each table also run **in parallel**.

If one table is still too heavy: add a **`tableOverrides`** entry with **`maxRows`** for a sampled compare, or list only the tables you need under **`tablesToCompare`**.

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE).
