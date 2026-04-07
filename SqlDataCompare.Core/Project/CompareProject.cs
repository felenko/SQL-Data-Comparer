using System.Text.Json.Serialization;

namespace SqlDataCompare.Project;

public sealed class CompareProject
{
    public int Version { get; set; } = 1;
    public string? Name { get; set; }
    public DataEndpoint Source { get; set; } = null!;
    public DataEndpoint Destination { get; set; } = null!;
    public CompareOptions Options { get; set; } = new();
    /// <summary>
    /// Optional explicit list of source tables to compare. When empty, every table discovered on the source is considered.
    /// Destination schema/table can be omitted to use the same name as on the source.
    /// </summary>
    public List<TablePairSelection> TablesToCompare { get; set; } = new();
    public List<TableOverride> TableOverrides { get; set; } = new();
}

public sealed class TablePairSelection
{
    public string? SourceSchema { get; set; }
    public required string SourceTable { get; set; }
    public string? DestSchema { get; set; }
    public string? DestTable { get; set; }
}

public sealed class CompareOptions
{
    public bool OrdinalIgnoreCase { get; set; } = true;
    public bool TrimStrings { get; set; }
    public int MaxReportedDiffsPerTable { get; set; } = 1000;
    public int CommandTimeoutSeconds { get; set; } = 120;
    public string DefaultLogLevel { get; set; } = "Information";
    public string? LogFilePath { get; set; }
}

public sealed class TableOverride
{
    public string? SourceSchema { get; set; }
    public required string SourceTable { get; set; }
    public string? DestSchema { get; set; }
    public string? DestTable { get; set; }
    public List<string>? KeyColumns { get; set; }
    public List<string>? IgnoreColumns { get; set; }
    public Dictionary<string, string>? ColumnMap { get; set; }
    public string? WhereClause { get; set; }
    public int? MaxRows { get; set; }
    public string? InsertFilePath { get; set; }
}

[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(DatabaseEndpoint), "database")]
[JsonDerivedType(typeof(InsertFolderEndpoint), "insertFolder")]
public abstract class DataEndpoint
{
}

public sealed class DatabaseEndpoint : DataEndpoint
{
    /// <summary>sqlserver | postgresql | mysql</summary>
    public string Provider { get; set; } = "sqlserver";
    public string ConnectionString { get; set; } = "";
    /// <summary>
    /// When <see cref="ConnectionString"/> is empty/whitespace, this value is decrypted (Windows DPAPI user scope) and used.
    /// When plaintext is present, it takes precedence.
    /// </summary>
    public string? ConnectionStringDpapiBase64 { get; set; }
    public string? SchemaIncludePattern { get; set; }
}

public sealed class InsertFolderEndpoint : DataEndpoint
{
    public string RootPath { get; set; } = "";
    /// <summary>SqlServer | PostgreSql | MySql</summary>
    public string SqlDialect { get; set; } = "SqlServer";
    /// <summary>File naming: "{table}.sql" or "{schema}.{table}.sql"</summary>
    public string FileNaming { get; set; } = "{table}.sql";
    public string SearchPattern { get; set; } = "*.sql";
    /// <summary>Schema/database for MySQL folder sources when filenames are table-only; optional for SqlServer/Postgres (defaults dbo/public).</summary>
    public string? DefaultSchema { get; set; }
}
