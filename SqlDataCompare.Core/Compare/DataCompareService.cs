using System.Diagnostics;
using Microsoft.Extensions.Logging;
using SqlDataCompare.DataSources;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;
using SqlDataCompare.Sql;

namespace SqlDataCompare.Compare;

public sealed class DataCompareService
{
    public async Task<ProjectCompareResult> RunAsync(
        CompareProject project,
        ILogger logger,
        CancellationToken cancellationToken = default,
        IProgress<CompareProgressInfo>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(project);
        ArgumentNullException.ThrowIfNull(logger);
        project.Source ??= new DatabaseEndpoint();
        project.Destination ??= new DatabaseEndpoint();
        project.Options ??= new CompareOptions();
        project.TablesToCompare ??= new List<TablePairSelection>();
        project.TableOverrides ??= new List<TableOverride>();
        var nameComparer = project.Options.OrdinalIgnoreCase
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;
        var valueComparer = new ValueComparer(project.Options.OrdinalIgnoreCase, project.Options.TrimStrings);
        var results = new List<TablePairCompareResult>();
        var maxDiffs = project.Options.MaxReportedDiffsPerTable <= 0 ? 1000 : project.Options.MaxReportedDiffsPerTable;

        await using var sourceSide = CompareSide.Create(project.Source, project.Options, isSource: true);
        await using var destSide = CompareSide.Create(project.Destination, project.Options, isSource: false);

        try
        {
            IReadOnlyList<TableRef> sourceTables;
            try
            {
                sourceTables = await sourceSide.ListTablesAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"Source endpoint failed while listing tables (check connection string and login). {ex.Message}", ex);
            }

            IReadOnlyList<TableRef> destTables;
            try
            {
                destTables = await destSide.ListTablesAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"Destination endpoint failed while listing tables (check connection string and login). {ex.Message}", ex);
            }

            var destSet = destTables.ToHashSet(TableRefComparer.Instance);

            var overrideIndex = BuildOverrideIndex(project.TableOverrides, nameComparer);

            var work = BuildWorklist(project, sourceTables, nameComparer, results);

            var totalExpected = results.Count + work.Count;
            progress?.Report(new CompareProgressInfo
            {
                CompletedTables = 0,
                TotalTables = totalExpected,
                LatestTable = null,
            });

            for (var i = 0; i < results.Count; i++)
            {
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = i + 1,
                    TotalTables = totalExpected,
                    LatestTable = results[i],
                });
            }

            return await RunCompareWorkAsync(
                project,
                logger,
                sourceSide,
                destSide,
                destSet,
                overrideIndex,
                work,
                results,
                totalExpected,
                nameComparer,
                valueComparer,
                maxDiffs,
                cancellationToken,
                progress);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Compare stopped; returning {Count} completed table result(s).", results.Count);
            return new ProjectCompareResult
            {
                ProjectName = project.Name ?? "CompareProject",
                Tables = results,
                Cancelled = true,
            };
        }
    }

    private static async Task<ProjectCompareResult> RunCompareWorkAsync(
        CompareProject project,
        ILogger logger,
        CompareSide sourceSide,
        CompareSide destSide,
        HashSet<TableRef> destSet,
        Dictionary<string, TableOverride> overrideIndex,
        List<(TableRef Src, TablePairSelection? Sel)> work,
        List<TablePairCompareResult> results,
        int totalExpected,
        StringComparer nameComparer,
        ValueComparer valueComparer,
        int maxDiffs,
        CancellationToken cancellationToken,
        IProgress<CompareProgressInfo>? progress)
    {
        logger.LogInformation("Comparing {Count} table pair(s).", work.Count);

        var tableOrdinal = 0;
        foreach (var (srcTable, sel) in work)
        {
            tableOrdinal++;
            cancellationToken.ThrowIfCancellationRequested();
            logger.LogInformation("[{Current}/{Total}] {Src}", tableOrdinal, work.Count, srcTable.Display);
            var ov = FindOverride(overrideIndex, srcTable);
            if (ov?.SkipCompare == true)
            {
                var setupSkip = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = "(skipped)",
                    Status = TableCompareStatus.Skipped,
                    ErrorMessage = "Skipped at setup (table override).",
                };
                results.Add(setupSkip);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = setupSkip,
                });
                continue;
            }

            var dstTable = ResolveDestTable(srcTable, ov, sel, destSet, nameComparer);
            if (dstTable is null)
            {
                if (destSide is InsertFolderCompareSide)
                {
                    // INSERT file doesn't exist yet — treat as an empty destination.
                    // The compare will report every source row as MissingInDestination.
                    dstTable = srcTable;
                }
                else
                {
                    var skipUnmapped = new TablePairCompareResult
                    {
                        SourceTable = srcTable.Display,
                        DestinationTable = "(unmapped)",
                        Status = TableCompareStatus.Skipped,
                        ErrorMessage = "No matching destination table (add a table override or align names).",
                    };
                    results.Add(skipUnmapped);
                    progress?.Report(new CompareProgressInfo
                    {
                        CompletedTables = results.Count,
                        TotalTables = totalExpected,
                        LatestTable = skipUnmapped,
                    });
                    continue;
                }
            }

            if (ov?.WhereClause is not null && !WhereClauseValidator.IsPermitted(ov.WhereClause))
            {
                var whereErr = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Error,
                    ErrorMessage = "WHERE clause failed safety validation (no semicolons, comments, or new lines).",
                };
                results.Add(whereErr);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = whereErr,
                });
                continue;
            }

            var srcSchema = await sourceSide.GetSchemaAsync(srcTable, ov, cancellationToken);
            var dstSchema = await destSide.GetSchemaAsync(dstTable.Value, ov, cancellationToken);
            if (srcSchema is null || dstSchema is null)
            {
                var metaErr = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Error,
                    ErrorMessage = "Could not load table metadata for one side.",
                };
                results.Add(metaErr);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = metaErr,
                });
                continue;
            }

            // If destination is an INSERT folder and the file doesn't exist yet, the schema
            // has no columns. Mirror the source schema so key-mapping and column-pair building
            // work; every source row will then appear as MissingInDestination.
            if (destSide is InsertFolderCompareSide && dstSchema.Columns.Count == 0)
                dstSchema = srcSchema;

            var keys = PrimaryKeyResolver.Resolve(srcSchema, ov, nameComparer);
            if (keys.KeyColumns.Count == 0 || keys.Confidence == KeyConfidence.Ambiguous)
            {
                var keySkip = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Skipped,
                    ErrorMessage = keys.Detail ?? "Key columns could not be resolved.",
                    Keys = new KeyResolutionSummary
                    {
                        Confidence = keys.Confidence.ToString(),
                        Columns = keys.KeyColumns,
                        Detail = keys.Detail,
                    },
                };
                results.Add(keySkip);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = keySkip,
                });
                continue;
            }

            if (!keys.KeyColumns.All(k => srcSchema.Columns.Any(c => nameComparer.Equals(c.Name, k))))
            {
                var keyMissing = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Error,
                    ErrorMessage = "A resolved key column is missing on the source table.",
                    Keys = new KeyResolutionSummary
                    {
                        Confidence = keys.Confidence.ToString(),
                        Columns = keys.KeyColumns,
                        Detail = keys.Detail,
                    },
                };
                results.Add(keyMissing);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = keyMissing,
                });
                continue;
            }

            var destKeyColumns = MapSourceKeysToDestination(keys.KeyColumns, dstSchema, ov?.ColumnMap, nameComparer);
            if (destKeyColumns is null)
            {
                var mapErr = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Error,
                    ErrorMessage = "Could not map key columns to destination (check column mapping / overrides).",
                    Keys = new KeyResolutionSummary
                    {
                        Confidence = keys.Confidence.ToString(),
                        Columns = keys.KeyColumns,
                        Detail = keys.Detail,
                    },
                };
                results.Add(mapErr);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = mapErr,
                });
                continue;
            }

            var valuePairs = CompareColumnPairBuilder.BuildValueColumnPairs(
                srcSchema, dstSchema, ov, keys.KeyColumns, nameComparer, project.Options.SkipBinaryColumnsInCompare);
            if (valuePairs.Count == 0)
            {
                logger.LogWarning("No comparable value columns for {Src}->{Dst}", srcTable.Display, dstTable.Value.Display);
            }

            var maxRows = ov?.MaxRows;
            var where = project.Source is DatabaseEndpoint ? ov?.WhereClause : null;
            var destWhere = project.Destination is DatabaseEndpoint ? ov?.WhereClause : null;
            if (project.Source is InsertFolderEndpoint)
                where = null;
            if (project.Destination is InsertFolderEndpoint)
                destWhere = null;

            List<Dictionary<string, object?>> srcRows;
            List<Dictionary<string, object?>> dstRows;
            try
            {
                var srcProject = keys.KeyColumns.Concat(valuePairs.Select(p => p.SourceColumn))
                    .Distinct(StringComparer.Ordinal).ToList();
                var dstProject = destKeyColumns.Concat(valuePairs.Select(p => p.DestinationColumn))
                    .Distinct(StringComparer.Ordinal).ToList();
                var loadSw = Stopwatch.StartNew();
                var srcTask = sourceSide.LoadRowsAsync(srcTable, ov, srcProject, keys.KeyColumns, where, maxRows,
                    cancellationToken);
                var dstTask = destSide.LoadRowsAsync(dstTable.Value, ov, dstProject, destKeyColumns, destWhere,
                    maxRows, cancellationToken);
                await Task.WhenAll(srcTask, dstTask);
                srcRows = await srcTask;
                dstRows = await dstTask;
                loadSw.Stop();
                logger.LogInformation(
                    "[{Current}/{Total}] Loaded rows for {Src}: source={SrcN}, destination={DstN} in {LoadMs}ms; merging…",
                    tableOrdinal, work.Count, srcTable.Display, srcRows.Count, dstRows.Count, loadSw.ElapsedMilliseconds);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed reading rows for {Table}", srcTable.Display);
                var readErr = new TablePairCompareResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = TableCompareStatus.Error,
                    ErrorMessage = ex.Message,
                };
                results.Add(readErr);
                progress?.Report(new CompareProgressInfo
                {
                    CompletedTables = results.Count,
                    TotalTables = totalExpected,
                    LatestTable = readErr,
                });
                continue;
            }

            var sampled = maxRows is > 0;
            var keySummary = new KeyResolutionSummary
            {
                Confidence = keys.Confidence.ToString(),
                Columns = keys.KeyColumns,
                Detail = keys.Detail,
            };

            IProgress<(int Compared, int Total)>? mergeProgress = null;
            if (progress is not null)
            {
                mergeProgress = new Progress<(int Compared, int Total)>(v =>
                    progress.Report(new CompareProgressInfo
                    {
                        CompletedTables = results.Count,
                        TotalTables = totalExpected,
                        LatestTable = null,
                        ActiveSourceTable = srcTable.Display,
                        RowsCompared = v.Compared,
                        RowsTotal = v.Total,
                    }));
            }

            var mergeSw = Stopwatch.StartNew();
            var compare = RowMergeComparer.Compare(
                srcRows,
                dstRows,
                keys.KeyColumns,
                destKeyColumns,
                valuePairs,
                valueComparer,
                maxDiffs,
                sampled,
                srcTable.Display,
                dstTable.Value.Display,
                keySummary,
                mergeProgress,
                cancellationToken);
            mergeSw.Stop();
            results.Add(compare);
            logger.LogInformation(
                "[{Current}/{Total}] Done {Src}: {Status} (merge {MergeMs}ms)",
                tableOrdinal, work.Count, srcTable.Display, compare.Status, mergeSw.ElapsedMilliseconds);
            progress?.Report(new CompareProgressInfo
            {
                CompletedTables = results.Count,
                TotalTables = totalExpected,
                LatestTable = compare,
                ActiveSourceTable = null,
                RowsCompared = null,
                RowsTotal = null,
            });
        }

        return new ProjectCompareResult
        {
            ProjectName = project.Name ?? "CompareProject",
            Tables = results,
            Cancelled = false,
        };
    }

    private static List<(TableRef Src, TablePairSelection? Sel)> BuildWorklist(
        CompareProject project,
        IReadOnlyList<TableRef> sourceTables,
        StringComparer nameComparer,
        List<TablePairCompareResult> results)
    {
        if (project.TablesToCompare.Count == 0)
        {
            return sourceTables
                .OrderBy(t => t.Schema, nameComparer).ThenBy(t => t.Name, nameComparer)
                .Select(t => (t, (TablePairSelection?)null))
                .ToList();
        }

        var list = new List<(TableRef, TablePairSelection?)>();
        foreach (var sel in project.TablesToCompare
                     .OrderBy(s => s.SourceSchema ?? "", nameComparer)
                     .ThenBy(s => s.SourceTable, nameComparer))
        {
            if (string.IsNullOrWhiteSpace(sel.SourceTable))
                continue;
            if (sel.Skip)
            {
                var skipLabel = string.IsNullOrWhiteSpace(sel.SourceSchema)
                    ? sel.SourceTable.Trim()
                    : $"{sel.SourceSchema}.{sel.SourceTable}";
                results.Add(new TablePairCompareResult
                {
                    SourceTable = skipLabel,
                    DestinationTable = "(skipped)",
                    Status = TableCompareStatus.Skipped,
                    ErrorMessage = "Skipped at setup.",
                });
                continue;
            }

            if (!TryResolveSourceTable(sourceTables, sel, nameComparer, out var srcTable, out var err))
            {
                results.Add(new TablePairCompareResult
                {
                    SourceTable = string.IsNullOrWhiteSpace(sel.SourceSchema)
                        ? sel.SourceTable
                        : $"{sel.SourceSchema}.{sel.SourceTable}",
                    DestinationTable = "(source not found)",
                    Status = TableCompareStatus.Error,
                    ErrorMessage = err,
                });
                continue;
            }

            list.Add((srcTable, sel));
        }

        return list;
    }

    private static bool TryResolveSourceTable(
        IReadOnlyList<TableRef> sourceTables,
        TablePairSelection sel,
        StringComparer comparer,
        out TableRef srcTable,
        out string? error)
    {
        error = null;
        srcTable = default;
        var matches = sourceTables.Where(t =>
            comparer.Equals(t.Name, sel.SourceTable) &&
            (string.IsNullOrWhiteSpace(sel.SourceSchema) ||
             comparer.Equals(t.Schema, sel.SourceSchema))).ToList();
        if (matches.Count == 0)
        {
            error = string.IsNullOrWhiteSpace(sel.SourceSchema)
                ? $"No source table named '{sel.SourceTable}' (set source schema if names collide across schemas)."
                : $"No source table '{sel.SourceSchema}.{sel.SourceTable}'.";
            return false;
        }

        if (matches.Count > 1)
        {
            error =
                $"Multiple source tables named '{sel.SourceTable}'; disambiguate with source schema ({string.Join(", ", matches.Select(m => m.Schema).Distinct(comparer))}).";
            return false;
        }

        srcTable = matches[0];
        return true;
    }

    private static Dictionary<string, TableOverride> BuildOverrideIndex(
        IEnumerable<TableOverride> overrides,
        StringComparer comparer) =>
        overrides
            .GroupBy(o => TableOverrideKey(o), comparer)
            .ToDictionary(g => g.Key, g => g.First(), comparer);

    private static string TableOverrideKey(TableOverride o)
    {
        var schema = o.SourceSchema ?? "";
        return $"{schema}\u001F{o.SourceTable}";
    }

    private static TableOverride? FindOverride(Dictionary<string, TableOverride> index, TableRef src)
    {
        var k1 = $"{src.Schema}\u001F{src.Name}";
        if (index.TryGetValue(k1, out var o))
            return o;
        var k2 = $"\u001F{src.Name}";
        return index.TryGetValue(k2, out o) ? o : null;
    }

    private static TableRef? ResolveDestTable(
        TableRef src,
        TableOverride? ov,
        TablePairSelection? sel,
        HashSet<TableRef> destSet,
        StringComparer comparer)
    {
        var destTableName = sel?.DestTable ?? ov?.DestTable;
        if (destTableName is not null)
        {
            var ds = sel?.DestSchema ?? ov?.DestSchema ?? src.Schema;
            var cand = new TableRef(ds, destTableName);
            foreach (var d in destSet)
            {
                if (comparer.Equals(d.Schema, cand.Schema) && comparer.Equals(d.Name, cand.Name))
                    return d;
            }

            return cand;
        }

        foreach (var k in destSet)
        {
            if (comparer.Equals(k.Schema, src.Schema) && comparer.Equals(k.Name, src.Name))
                return k;
        }

        return null;
    }

    private static IReadOnlyList<string>? MapSourceKeysToDestination(
        IReadOnlyList<string> sourceKeys,
        TableSchema dstSchema,
        Dictionary<string, string>? columnMap,
        StringComparer comparer)
    {
        var list = new List<string>();
        foreach (var sk in sourceKeys)
        {
            string? destName = null;
            if (columnMap is not null)
            {
                foreach (var (s, d) in columnMap)
                {
                    if (!comparer.Equals(s, sk)) continue;
                    destName = dstSchema.Columns.Select(c => c.Name).FirstOrDefault(dc => comparer.Equals(dc, d));
                    break;
                }
            }

            destName ??= dstSchema.Columns.Select(c => c.Name).FirstOrDefault(dc => comparer.Equals(dc, sk));
            if (destName is null)
                return null;
            list.Add(destName);
        }

        return list;
    }

}

internal sealed class TableRefComparer : IEqualityComparer<TableRef>
{
    public static readonly TableRefComparer Instance = new();
    private readonly StringComparer _c = StringComparer.OrdinalIgnoreCase;

    public bool Equals(TableRef x, TableRef y) =>
        _c.Equals(x.Schema, y.Schema) && _c.Equals(x.Name, y.Name);

    public int GetHashCode(TableRef obj) => HashCode.Combine(_c.GetHashCode(obj.Schema), _c.GetHashCode(obj.Name));
}

internal abstract class CompareSide : IAsyncDisposable
{
    public static CompareSide Create(DataEndpoint endpoint, CompareOptions options, bool isSource) =>
        endpoint switch
        {
            DatabaseEndpoint d => new DatabaseCompareSide(d, options),
            InsertFolderEndpoint f => new InsertFolderCompareSide(f),
            _ => throw new ArgumentOutOfRangeException(nameof(endpoint)),
        };

    public abstract Task<IReadOnlyList<TableRef>> ListTablesAsync(CancellationToken cancellationToken);
    public abstract Task<TableSchema?> GetSchemaAsync(TableRef table, TableOverride? ov, CancellationToken cancellationToken);

    public abstract Task<List<Dictionary<string, object?>>> LoadRowsAsync(
        TableRef table,
        TableOverride? ov,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken);

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class DatabaseCompareSide : CompareSide
{
    private readonly IDatabaseGateway _gateway;
    private readonly DatabaseEndpoint _endpoint;

    public DatabaseCompareSide(DatabaseEndpoint endpoint, CompareOptions options)
    {
        _endpoint = endpoint;
        _gateway = DatabaseGatewayFactory.Create(endpoint, options);
    }

    public override async Task<IReadOnlyList<TableRef>> ListTablesAsync(CancellationToken cancellationToken) =>
        await _gateway.ListTablesAsync(_endpoint.SchemaIncludePattern, cancellationToken);

    public override async Task<TableSchema?> GetSchemaAsync(TableRef table, TableOverride? ov, CancellationToken cancellationToken) =>
        await _gateway.GetTableSchemaAsync(table, cancellationToken);

    public override async Task<List<Dictionary<string, object?>>> LoadRowsAsync(
        TableRef table,
        TableOverride? ov,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken)
    {
        var rows = await _gateway.ReadRowsOrderedAsync(table, projectedColumns, keyColumns, whereClause, maxRows,
            cancellationToken);
        return rows.ToList();
    }

    public override async ValueTask DisposeAsync()
    {
        await _gateway.DisposeAsync();
        await base.DisposeAsync();
    }
}

internal sealed class InsertFolderCompareSide : CompareSide
{
    private readonly InsertFolderDataAccess _folder;

    public InsertFolderCompareSide(InsertFolderEndpoint endpoint) =>
        _folder = new InsertFolderDataAccess(endpoint);

    public override Task<IReadOnlyList<TableRef>> ListTablesAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_folder.ListTables());

    public override Task<TableSchema?> GetSchemaAsync(TableRef table, TableOverride? ov, CancellationToken cancellationToken)
    {
        try
        {
            var schema = _folder.BuildSchemaFromInserts(table);
            return Task.FromResult<TableSchema?>(schema);
        }
        catch (FileNotFoundException)
        {
            // File doesn't exist yet (empty folder). Return an empty-column schema so the
            // caller can detect this and mirror the source schema, resulting in all source
            // rows appearing as MissingInDestination.
            return Task.FromResult<TableSchema?>(new TableSchema
            {
                Table = table,
                Columns = Array.Empty<ColumnDefinition>(),
                PrimaryKeyColumns = Array.Empty<string>(),
            });
        }
    }

    public override Task<List<Dictionary<string, object?>>> LoadRowsAsync(
        TableRef table,
        TableOverride? ov,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken)
    {
        _ = whereClause;
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            var all = _folder.LoadAllRows(table, ov?.InsertFilePath, maxRows, projectedColumns);
            var projected = all.Select(row =>
            {
                var d = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var c in projectedColumns)
                {
                    row.TryGetValue(c, out var v);
                    d[c] = v;
                }
                return d;
            }).ToList();
            return Task.FromResult(projected);
        }
        catch (FileNotFoundException)
        {
            return Task.FromResult(new List<Dictionary<string, object?>>());
        }
    }
}
