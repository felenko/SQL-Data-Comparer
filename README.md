# SQL Data Comparer

Compare row-level data between two **SQL Server**, **PostgreSQL**, or **MySQL** databases, or between **folders of `INSERT` scripts**, from a **WPF desktop app** or a **command-line** runner. Optional reporting to JSON, HTML, CSV, and JUnit-style XML.

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

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE).
