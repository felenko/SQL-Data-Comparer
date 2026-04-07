using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SqlDataCompare.Compare;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;
using SqlDataCompare.Sync;

namespace SqlDataCompare.Mcp;

/// <summary>JSON helpers for the MCP host and other automation callers.</summary>
public static class McpDatabaseTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public static async Task<string> EnumerateTablesJsonAsync(
        string connectionString,
        string provider = "sqlserver",
        int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default)
    {
        var ep = new DatabaseEndpoint { Provider = provider, ConnectionString = connectionString };
        var opt = new CompareOptions { CommandTimeoutSeconds = commandTimeoutSeconds };
        var tables = await CompareTableDiscovery.ListTablesAsync(ep, opt, cancellationToken);
        var list = tables.Select(t => new { schema = t.Schema, name = t.Name, display = t.Display }).ToList();
        return JsonSerializer.Serialize(list, JsonOpts);
    }

    /// <summary>
    /// Reads row data (all columns, ordered by key) and lists FK relationships touching the table.
    /// <paramref name="maxRows"/> caps rows returned (default 5000).
    /// </summary>
    public static async Task<string> GetTableDataWithRelationsJsonAsync(
        string connectionString,
        string tableSchema,
        string tableName,
        string provider = "sqlserver",
        int maxRows = 5000,
        int commandTimeoutSeconds = 120,
        CancellationToken cancellationToken = default)
    {
        if (maxRows <= 0)
            maxRows = 5000;
        maxRows = Math.Min(maxRows, 100_000);

        var ep = new DatabaseEndpoint { Provider = provider, ConnectionString = connectionString };
        var opt = new CompareOptions { CommandTimeoutSeconds = commandTimeoutSeconds };
        var nameComparer = StringComparer.OrdinalIgnoreCase;

        await using var side = CompareSide.Create(ep, opt, isSource: true);
        var tables = await side.ListTablesAsync(cancellationToken);
        var tref = tables.FirstOrDefault(t =>
            nameComparer.Equals(t.Schema, tableSchema) && nameComparer.Equals(t.Name, tableName));
        if (tref == default)
            return JsonSerializer.Serialize(new { error = $"Table '{tableSchema}.{tableName}' not found." }, JsonOpts);

        var tableSchemaModel = await side.GetSchemaAsync(tref, null, cancellationToken);
        if (tableSchemaModel is null)
            return JsonSerializer.Serialize(new { error = "Could not read table metadata." }, JsonOpts);

        var keys = PrimaryKeyResolver.Resolve(tableSchemaModel, null, nameComparer);
        if (keys.KeyColumns.Count == 0 || keys.Confidence == KeyConfidence.Ambiguous)
            return JsonSerializer.Serialize(
                new { error = keys.Detail ?? "Could not resolve key columns.", keys = keys.KeyColumns }, JsonOpts);

        var allCols = tableSchemaModel.Columns.Select(c => c.Name).Distinct(StringComparer.Ordinal).ToList();
        var rows = await side.LoadRowsAsync(tref, null, allCols, keys.KeyColumns, null, maxRows, cancellationToken);

        var (outgoing, incoming) = await RelatedTableDiscovery.GetEdgesInvolvingTableAsync(ep, opt, tref.Schema, tref.Name,
            cancellationToken);

        var payload = new
        {
            table = tref.Display,
            keyColumns = keys.KeyColumns,
            keyConfidence = keys.Confidence.ToString(),
            maxRows,
            rowCountReturned = rows.Count,
            truncated = rows.Count >= maxRows,
            rows,
            relatedTables = new
            {
                outgoingForeignKeys = outgoing.Select(e => new
                {
                    referencing = e.ReferencingDisplay,
                    referenced = e.ReferencedDisplay,
                }),
                incomingForeignKeys = incoming.Select(e => new
                {
                    referencing = e.ReferencingDisplay,
                    referenced = e.ReferencedDisplay,
                }),
            },
        };

        return JsonSerializer.Serialize(payload, JsonOpts);
    }

    public static async Task<string> CompareTwoTablesJsonAsync(
        string sourceConnectionString,
        string destinationConnectionString,
        string sourceSchema,
        string sourceTable,
        string? destinationSchema,
        string? destinationTable,
        string provider = "sqlserver",
        int maxReportedDiffsPerTable = 1000,
        int commandTimeoutSeconds = 120,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        logger ??= NullLogger.Instance;
        var project = new CompareProject
        {
            Name = "McpCompare",
            Source = new DatabaseEndpoint { Provider = provider, ConnectionString = sourceConnectionString },
            Destination = new DatabaseEndpoint { Provider = provider, ConnectionString = destinationConnectionString },
            Options = new CompareOptions
            {
                OrdinalIgnoreCase = true,
                SkipBinaryColumnsInCompare = true,
                MaxReportedDiffsPerTable = maxReportedDiffsPerTable <= 0 ? 1000 : maxReportedDiffsPerTable,
                CommandTimeoutSeconds = commandTimeoutSeconds <= 0 ? 120 : commandTimeoutSeconds,
            },
            TablesToCompare =
            [
                new TablePairSelection
                {
                    SourceSchema = sourceSchema,
                    SourceTable = sourceTable,
                    DestSchema = string.IsNullOrWhiteSpace(destinationSchema) ? null : destinationSchema,
                    DestTable = string.IsNullOrWhiteSpace(destinationTable) ? null : destinationTable,
                },
            ],
        };

        var svc = new DataCompareService();
        var result = await svc.RunAsync(project, logger, cancellationToken);
        return JsonSerializer.Serialize(result, JsonOpts);
    }

    /// <summary>
    /// Applies sync for specific rows on one table. <paramref name="rowsJson"/> is a JSON array of objects
    /// with key column values, e.g. <c>[{"Id":1},{"Id":2}]</c>.
    /// <paramref name="rowOperationKind"/>: MissingInDestination = insert source row into destination, ValueMismatch = update, MissingInSource = delete on destination.
    /// If <paramref name="keyColumnsJson"/> is null/empty, primary key columns are read from the source catalog.
    /// </summary>
    public static async Task<string> CopySelectedRowsJsonAsync(
        string sourceConnectionString,
        string destinationConnectionString,
        string sourceSchema,
        string sourceTable,
        string rowsJson,
        RowDifferenceKind rowOperationKind,
        string? destinationSchema = null,
        string? destinationTable = null,
        string? keyColumnsJson = null,
        string provider = "sqlserver",
        bool insertMissing = true,
        bool updateChanged = false,
        bool deleteExtra = false,
        bool disableForeignKeyChecks = false,
        int commandTimeoutSeconds = 120,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        logger ??= NullLogger.Instance;

        List<Dictionary<string, JsonElement>>? rawRows;
        try
        {
            rawRows = JsonSerializer.Deserialize<List<Dictionary<string, JsonElement>>>(rowsJson);
        }
        catch (JsonException ex)
        {
            return JsonSerializer.Serialize(new { error = "Invalid rowsJson.", detail = ex.Message }, JsonOpts);
        }

        if (rawRows is null || rawRows.Count == 0)
            return JsonSerializer.Serialize(new { error = "rowsJson must be a non-empty JSON array of objects." }, JsonOpts);

        IReadOnlyList<string>? keyCols = null;
        if (!string.IsNullOrWhiteSpace(keyColumnsJson) && keyColumnsJson.Trim() != "[]")
        {
            try
            {
                keyCols = JsonSerializer.Deserialize<List<string>>(keyColumnsJson);
            }
            catch (JsonException ex)
            {
                return JsonSerializer.Serialize(new { error = "Invalid keyColumnsJson.", detail = ex.Message }, JsonOpts);
            }

            if (keyCols is null || keyCols.Count == 0)
                keyCols = null;
        }

        var nameComparer = StringComparer.OrdinalIgnoreCase;
        var opt = new CompareOptions
        {
            CommandTimeoutSeconds = commandTimeoutSeconds <= 0 ? 120 : commandTimeoutSeconds,
            SkipBinaryColumnsInCompare = true,
        };
        var srcEp = new DatabaseEndpoint { Provider = provider, ConnectionString = sourceConnectionString };

        if (keyCols is null)
        {
            await using var side = CompareSide.Create(srcEp, opt, isSource: true);
            var tables = await side.ListTablesAsync(cancellationToken);
            var tref = tables.FirstOrDefault(t =>
                nameComparer.Equals(t.Schema, sourceSchema) && nameComparer.Equals(t.Name, sourceTable));
            if (tref == default)
                return JsonSerializer.Serialize(new { error = $"Source table '{sourceSchema}.{sourceTable}' not found." },
                    JsonOpts);
            var sch = await side.GetSchemaAsync(tref, null, cancellationToken);
            if (sch is null)
                return JsonSerializer.Serialize(new { error = "Could not load source table schema." }, JsonOpts);
            var keys = PrimaryKeyResolver.Resolve(sch, null, nameComparer);
            if (keys.KeyColumns.Count == 0 || keys.Confidence == KeyConfidence.Ambiguous)
                return JsonSerializer.Serialize(new { error = keys.Detail ?? "Could not resolve key columns." }, JsonOpts);
            keyCols = keys.KeyColumns;
        }

        var srcRef = new TableRef(sourceSchema, sourceTable);
        var tableDisplay = srcRef.Display;

        var keySet = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in rawRows)
        {
            var row = new Dictionary<string, object?>(nameComparer);
            foreach (var (k, v) in raw)
                row[k] = JsonElementToObject(v);

            var keyParts = keyCols.Select(k =>
                row.TryGetValue(k, out var v) ? v?.ToString() ?? "\u0000NULL" : "\u0000MISSING");
            var keyDisplay = string.Join('\u001F', keyParts);
            keySet.Add(SyncSelection.FormatRowKey(rowOperationKind, keyDisplay));
        }

        var project = new CompareProject
        {
            Name = "McpCopyRows",
            Source = srcEp,
            Destination = new DatabaseEndpoint
            {
                Provider = provider,
                ConnectionString = destinationConnectionString,
            },
            Options = opt,
            TablesToCompare =
            [
                new TablePairSelection
                {
                    SourceSchema = sourceSchema,
                    SourceTable = sourceTable,
                    DestSchema = string.IsNullOrWhiteSpace(destinationSchema) ? null : destinationSchema,
                    DestTable = string.IsNullOrWhiteSpace(destinationTable) ? null : destinationTable,
                },
            ],
        };

        var syncOptions = new SyncOptions
        {
            InsertMissing = insertMissing,
            UpdateChanged = updateChanged,
            DeleteExtra = deleteExtra,
            DisableForeignKeyChecks = disableForeignKeyChecks,
            Selection = new SyncSelection
            {
                IncludedSourceTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { tableDisplay },
                RowsBySourceTable = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
                {
                    [tableDisplay] = keySet,
                },
            },
        };

        var sync = new DataSyncService();
        var syncResult = await sync.RunAsync(project, syncOptions, logger, cancellationToken);
        return JsonSerializer.Serialize(syncResult, JsonOpts);
    }

    /// <summary>
    /// Sync all row differences from source to destination for every table in the project (full merge; not limited to compare samples).
    /// Destination must be a database endpoint. Respects project table list, overrides, and FK ordering.
    /// </summary>
    public static async Task<string> CopyProjectDifferencesJsonAsync(
        string projectJson,
        bool insertMissing = true,
        bool updateChanged = true,
        bool deleteExtra = false,
        bool disableForeignKeyChecks = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        logger ??= NullLogger.Instance;

        CompareProject project;
        try
        {
            project = CompareProjectSerializer.Parse(projectJson);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or ArgumentException)
        {
            return JsonSerializer.Serialize(new { error = "Invalid projectJson.", detail = ex.Message }, JsonOpts);
        }

        if (project.Destination is not DatabaseEndpoint)
            return JsonSerializer.Serialize(
                new { error = "Sync requires a database destination (JSON kind: database), not an INSERT folder." }, JsonOpts);

        if (!insertMissing && !updateChanged && !deleteExtra)
            return JsonSerializer.Serialize(
                new
                {
                    error =
                        "At least one of insertMissing, updateChanged, or deleteExtra must be true.",
                }, JsonOpts);

        var syncOptions = new SyncOptions
        {
            InsertMissing = insertMissing,
            UpdateChanged = updateChanged,
            DeleteExtra = deleteExtra,
            DisableForeignKeyChecks = disableForeignKeyChecks,
            Selection = null,
        };

        var sync = new DataSyncService();
        var syncResult = await sync.RunAsync(project, syncOptions, logger, cancellationToken);
        return JsonSerializer.Serialize(syncResult, JsonOpts);
    }

    private static object? JsonElementToObject(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out var l) ? l : e.TryGetDouble(out var d) ? d : e.GetRawText(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        _ => e.GetRawText(),
    };
}
