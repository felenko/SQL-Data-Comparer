using System.ComponentModel;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using SqlDataCompare.Compare;
using SqlDataCompare.Mcp;

namespace SqlDataCompare.McpHost;

/// <summary>
/// MCP tools for SQL Data Compare. Uses stdio; logs must go to stderr (see Program.cs).
/// Connection strings are passed per call — treat as secrets.
/// </summary>
[McpServerToolType]
public static class SqlDataCompareMcpTools
{
    [McpServerTool]
    [Description(
        "Enumerate tables on a database. Returns JSON array of { schema, name, display }. Provider: sqlserver, postgresql, mysql.")]
    public static async Task<string> SqlEnumerateTables(
        [Description("ADO.NET connection string")] string connectionString,
        [Description("sqlserver (default), postgresql, or mysql")] string provider = "sqlserver",
        [Description("Per-command timeout in seconds")] int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default) =>
        await McpDatabaseTools.EnumerateTablesJsonAsync(connectionString, provider, commandTimeoutSeconds,
            cancellationToken);

    [McpServerTool]
    [Description(
        "Load rows from a table (all columns, ordered by primary key) and list foreign-key related tables (incoming/outgoing). Returns JSON. maxRows caps payload size (default 5000, max 100000).")]
    public static async Task<string> SqlGetTableDataWithRelations(
        [Description("ADO.NET connection string")] string connectionString,
        [Description("Table schema (e.g. dbo)")] string tableSchema,
        [Description("Table name")] string tableName,
        [Description("sqlserver, postgresql, or mysql")] string provider = "sqlserver",
        [Description("Maximum rows to return")] int maxRows = 5000,
        [Description("Per-command timeout in seconds")] int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default) =>
        await McpDatabaseTools.GetTableDataWithRelationsJsonAsync(connectionString, tableSchema, tableName, provider,
            maxRows, commandTimeoutSeconds, cancellationToken);

    [McpServerTool]
    [Description(
        "Compare one source table to a destination table. Returns JSON ProjectCompareResult with per-column value diffs and row samples. Leave destinationSchema/destinationTable empty to use same names as source.")]
    public static async Task<string> SqlCompareTwoTables(
        [Description("Source connection string")] string sourceConnectionString,
        [Description("Destination connection string")] string destinationConnectionString,
        [Description("Source table schema")] string sourceSchema,
        [Description("Source table name")] string sourceTable,
        [Description("Destination schema, or empty")] string? destinationSchema = null,
        [Description("Destination table, or empty")] string? destinationTable = null,
        [Description("sqlserver, postgresql, or mysql")] string provider = "sqlserver",
        [Description("Cap on sampled differing rows per table")] int maxReportedDiffsPerTable = 1000,
        [Description("Per-command timeout in seconds")] int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default) =>
        await McpDatabaseTools.CompareTwoTablesJsonAsync(
            sourceConnectionString,
            destinationConnectionString,
            sourceSchema,
            sourceTable,
            destinationSchema,
            destinationTable,
            provider,
            maxReportedDiffsPerTable,
            commandTimeoutSeconds,
            NullLogger.Instance,
            cancellationToken);

    [McpServerTool]
    [Description(
        "Copy/sync selected rows from source table to destination (writes destination only). rowsJson is a JSON array of objects with key columns, e.g. [{\"Id\":1}]. rowOperationKind: MissingInDestination = insert missing rows, ValueMismatch = push updates, MissingInSource = delete extra on destination. Enable matching sync flags (insertMissing/updateChanged/deleteExtra). Optional keyColumnsJson e.g. [\"Id\"] overrides PK detection.")]
    public static async Task<string> SqlCopySelectedRowsToDestination(
        [Description("Source connection string (read)")] string sourceConnectionString,
        [Description("Destination connection string (write)")] string destinationConnectionString,
        [Description("Source schema")] string sourceSchema,
        [Description("Source table name")] string sourceTable,
        [Description("JSON array of row key objects")] string rowsJson,
        [Description("MissingInDestination | ValueMismatch | MissingInSource")] string rowOperationKind,
        [Description("Destination schema, or empty")] string? destinationSchema = null,
        [Description("Destination table, or empty")] string? destinationTable = null,
        [Description("Optional JSON array of key column names; empty = use catalog PK")] string? keyColumnsJson = null,
        [Description("sqlserver, postgresql, or mysql")] string provider = "sqlserver",
        [Description("Allow INSERT for keys only on source")] bool insertMissing = true,
        [Description("Allow UPDATE when values differ")] bool updateChanged = false,
        [Description("Allow DELETE on destination for keys not on source")] bool deleteExtra = false,
        [Description("Temporarily relax FK checks on destination if needed")] bool disableForeignKeyChecks = false,
        [Description("Per-command timeout in seconds")] int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default)
    {
        if (!Enum.TryParse<RowDifferenceKind>(rowOperationKind, ignoreCase: true, out var kind))
        {
            return """{"error":"Invalid rowOperationKind. Use MissingInDestination, ValueMismatch, or MissingInSource."}""";
        }

        return await McpDatabaseTools.CopySelectedRowsJsonAsync(
            sourceConnectionString,
            destinationConnectionString,
            sourceSchema,
            sourceTable,
            rowsJson,
            kind,
            destinationSchema,
            destinationTable,
            keyColumnsJson,
            provider,
            insertMissing,
            updateChanged,
            deleteExtra,
            disableForeignKeyChecks,
            commandTimeoutSeconds,
            NullLogger.Instance,
            cancellationToken);
    }

    [McpServerTool]
    [Description(
        "Copy/sync all data differences from source to destination using a full compare project JSON (same format as a saved .json project file). Writes destination only. Runs a full table merge per table (not limited to compare sample rows). Enable insertMissing/updateChanged/deleteExtra as needed; deleteExtra removes destination rows missing on source.")]
    public static async Task<string> SqlCopyProjectDifferencesToDestination(
        [Description("Compare project JSON (source/destination endpoints, options, tablesToCompare, tableOverrides)")]
        string projectJson,
        [Description("INSERT rows present on source but missing on destination")] bool insertMissing = true,
        [Description("UPDATE rows where non-key values differ")] bool updateChanged = true,
        [Description("DELETE rows on destination that are not on source")] bool deleteExtra = false,
        [Description("Temporarily relax FK checks on destination if needed")] bool disableForeignKeyChecks = false,
        CancellationToken cancellationToken = default) =>
        await McpDatabaseTools.CopyProjectDifferencesJsonAsync(
            projectJson,
            insertMissing,
            updateChanged,
            deleteExtra,
            disableForeignKeyChecks,
            NullLogger.Instance,
            cancellationToken);
}
