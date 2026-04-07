using Microsoft.Extensions.Logging;
using SqlDataCompare.Compare;
using SqlDataCompare.DataSources;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Sync;

public sealed class DataSyncService
{
    public async Task<ProjectSyncResult> RunAsync(
        CompareProject project,
        SyncOptions syncOptions,
        ILogger logger,
        CancellationToken cancellationToken = default,
        IProgress<SyncProgressInfo>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(project);
        ArgumentNullException.ThrowIfNull(syncOptions);
        ArgumentNullException.ThrowIfNull(logger);

        project.Source ??= new DatabaseEndpoint();
        project.Destination ??= new DatabaseEndpoint();
        project.Options ??= new CompareOptions();
        project.TablesToCompare ??= [];
        project.TableOverrides ??= [];

        if (project.Destination is not DatabaseEndpoint destDbEndpoint)
            throw new InvalidOperationException("Sync only supports database destinations, not INSERT folder endpoints.");

        var nameComparer = project.Options.OrdinalIgnoreCase
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;
        var valueComparer = new ValueComparer(project.Options.OrdinalIgnoreCase, project.Options.TrimStrings);
        var provider = destDbEndpoint.Provider ?? "sqlserver";
        var results = new List<TableSyncResult>();

        await using var sourceSide = CompareSide.Create(project.Source, project.Options, isSource: true);
        await using var destSide = CompareSide.Create(project.Destination, project.Options, isSource: false);
        await using var destGateway = DatabaseGatewayFactory.Create(destDbEndpoint, project.Options);

        var sourceTables = await sourceSide.ListTablesAsync(cancellationToken);
        var destTables = await destSide.ListTablesAsync(cancellationToken);
        var destSet = destTables.ToHashSet(TableRefComparer.Instance);

        var overrideIndex = BuildOverrideIndex(project.TableOverrides, nameComparer);
        var work = BuildWorklist(project, sourceTables, nameComparer, results);

        var tablesTotal = work.Count;
        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.Started,
            TablesTotal = tablesTotal,
            TableIndex = 0,
        });

        var tableIndex = 0;
        foreach (var (srcTable, sel) in work)
        {
            cancellationToken.ThrowIfCancellationRequested();
            tableIndex++;
            var ov = FindOverride(overrideIndex, srcTable);
            var dstTable = ResolveDestTable(srcTable, ov, sel, destSet, nameComparer);
            if (dstTable is null)
            {
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableStarting,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = "(unmapped)",
                });
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = "(unmapped)",
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "No matching destination table.",
                });
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableCompleted,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = "(unmapped)",
                });
                continue;
            }

            if (syncOptions.Selection?.IncludedSourceTables is { Count: > 0 } includedTables &&
                !includedTables.Contains(srcTable.Display))
            {
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableStarting,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                });
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "Table not selected for sync.",
                });
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableCompleted,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                });
                continue;
            }

            progress?.Report(new SyncProgressInfo
            {
                Phase = SyncProgressPhase.TableStarting,
                TablesTotal = tablesTotal,
                TableIndex = tableIndex,
                SourceTable = srcTable.Display,
                DestinationTable = dstTable.Value.Display,
            });

            try
            {
                var syncResult = await SyncTableAsync(
                    srcTable, dstTable.Value, ov, sel,
                    sourceSide, destSide, destGateway,
                    project.Source, project.Destination,
                    nameComparer, valueComparer,
                    provider, syncOptions, logger,
                    tableIndex, tablesTotal, progress,
                    cancellationToken);
                results.Add(syncResult);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed syncing {Src} -> {Dst}", srcTable.Display, dstTable.Value.Display);
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Value.Display,
                    Status = SyncStatus.Error,
                    ErrorMessage = ex.Message,
                });
            }

            progress?.Report(new SyncProgressInfo
            {
                Phase = SyncProgressPhase.TableCompleted,
                TablesTotal = tablesTotal,
                TableIndex = tableIndex,
                SourceTable = srcTable.Display,
                DestinationTable = dstTable.Value.Display,
            });
        }

        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.Finished,
            TablesTotal = tablesTotal,
            TableIndex = tablesTotal,
        });

        return new ProjectSyncResult { Tables = results };
    }

    private static async Task<TableSyncResult> SyncTableAsync(
        TableRef srcTable,
        TableRef dstTable,
        TableOverride? ov,
        TablePairSelection? sel,
        CompareSide sourceSide,
        CompareSide destSide,
        IDatabaseGateway destGateway,
        DataEndpoint srcEndpoint,
        DataEndpoint dstEndpoint,
        StringComparer nameComparer,
        ValueComparer valueComparer,
        string provider,
        SyncOptions opts,
        ILogger logger,
        int tableIndex,
        int tablesTotal,
        IProgress<SyncProgressInfo>? progress,
        CancellationToken ct)
    {
        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.LoadingRows,
            TablesTotal = tablesTotal,
            TableIndex = tableIndex,
            SourceTable = srcTable.Display,
            DestinationTable = dstTable.Display,
        });

        var srcSchema = await sourceSide.GetSchemaAsync(srcTable, ov, ct);
        var dstSchema = await destSide.GetSchemaAsync(dstTable, ov, ct);
        if (srcSchema is null || dstSchema is null)
            throw new InvalidOperationException("Could not load table schema for one side.");

        var keys = PrimaryKeyResolver.Resolve(srcSchema, ov, nameComparer);
        if (keys.KeyColumns.Count == 0 || keys.Confidence == KeyConfidence.Ambiguous)
            return new TableSyncResult
            {
                SourceTable = srcTable.Display,
                DestinationTable = dstTable.Display,
                Status = SyncStatus.Skipped,
                ErrorMessage = keys.Detail ?? "Key columns could not be resolved.",
            };

        var destKeyColumns = MapSourceKeysToDestination(keys.KeyColumns, dstSchema, ov?.ColumnMap, nameComparer);
        if (destKeyColumns is null)
            throw new InvalidOperationException("Could not map key columns to destination.");

        var valuePairs = BuildValueColumnPairs(srcSchema, dstSchema, ov, keys.KeyColumns, nameComparer);

        var where = srcEndpoint is DatabaseEndpoint ? ov?.WhereClause : null;
        var destWhere = dstEndpoint is DatabaseEndpoint ? ov?.WhereClause : null;

        var srcProject = keys.KeyColumns.Concat(valuePairs.Select(p => p.SourceColumn))
            .Distinct(StringComparer.Ordinal).ToList();
        var dstProject = destKeyColumns.Concat(valuePairs.Select(p => p.DestinationColumn))
            .Distinct(StringComparer.Ordinal).ToList();

        var srcRows = await sourceSide.LoadRowsAsync(srcTable, ov, srcProject, keys.KeyColumns, where, null, ct);
        var dstRows = await destSide.LoadRowsAsync(dstTable, ov, dstProject, destKeyColumns, destWhere, null, ct);

        // Sort both by key (same approach as RowMergeComparer)
        var srcSorted = srcRows.OrderBy(r => KeyString(r, keys.KeyColumns), StringComparer.Ordinal).ToList();
        var dstSorted = dstRows.OrderBy(r => KeyString(r, destKeyColumns), StringComparer.Ordinal).ToList();

        var batch = new List<(string Sql, IReadOnlyList<object?> Params)>();

        // SQL Server needs IDENTITY_INSERT ON if any key column is an identity column on dest
        var needsIdentityInsert = DatabaseProviderNames.Parse(provider) == DatabaseProviderKind.SqlServer
            && keys.KeyColumns.Any(k =>
            {
                var destColName = destKeyColumns[keys.KeyColumns.ToList().IndexOf(k)];
                return dstSchema.Columns.Any(c => nameComparer.Equals(c.Name, destColName) && c.IsIdentity);
            });

        if (needsIdentityInsert)
            batch.Add(($"SET IDENTITY_INSERT {SyncSqlBuilder.QuoteTable(dstTable, provider)} ON", Array.Empty<object?>()));

        long inserted = 0, updated = 0, deleted = 0;
        int i = 0, j = 0;

        while (i < srcSorted.Count && j < dstSorted.Count)
        {
            ct.ThrowIfCancellationRequested();
            var ks = KeyString(srcSorted[i], keys.KeyColumns);
            var kd = KeyString(dstSorted[j], destKeyColumns);
            var cmp = string.CompareOrdinal(ks, kd);

            if (cmp < 0)
            {
                if (opts.InsertMissing &&
                    IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.MissingInDestination, ks))
                {
                    batch.Add(BuildInsert(srcSorted[i], dstTable, keys.KeyColumns, destKeyColumns, valuePairs, dstSchema, provider));
                    inserted++;
                }
                i++;
            }
            else if (cmp > 0)
            {
                if (opts.DeleteExtra &&
                    IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.MissingInSource, kd))
                {
                    batch.Add(BuildDelete(dstSorted[j], dstTable, destKeyColumns, provider));
                    deleted++;
                }
                j++;
            }
            else
            {
                if (opts.UpdateChanged)
                {
                    var hasDiff = valuePairs.Any(p =>
                    {
                        srcSorted[i].TryGetValue(p.SourceColumn, out var va);
                        dstSorted[j].TryGetValue(p.DestinationColumn, out var vb);
                        return !valueComparer.Equal(va, vb);
                    });
                    if (hasDiff &&
                        IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.ValueMismatch, ks))
                    {
                        batch.Add(BuildUpdate(srcSorted[i], dstSorted[j], dstTable, keys.KeyColumns, destKeyColumns, valuePairs, provider));
                        updated++;
                    }
                }
                i++;
                j++;
            }
        }

        while (i < srcSorted.Count)
        {
            ct.ThrowIfCancellationRequested();
            var ksTail = KeyString(srcSorted[i], keys.KeyColumns);
            if (opts.InsertMissing &&
                IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.MissingInDestination, ksTail))
            {
                batch.Add(BuildInsert(srcSorted[i], dstTable, keys.KeyColumns, destKeyColumns, valuePairs, dstSchema, provider));
                inserted++;
            }
            i++;
        }

        while (j < dstSorted.Count)
        {
            ct.ThrowIfCancellationRequested();
            var kdTail = KeyString(dstSorted[j], destKeyColumns);
            if (opts.DeleteExtra &&
                IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.MissingInSource, kdTail))
            {
                batch.Add(BuildDelete(dstSorted[j], dstTable, destKeyColumns, provider));
                deleted++;
            }
            j++;
        }

        if (needsIdentityInsert)
            batch.Add(($"SET IDENTITY_INSERT {SyncSqlBuilder.QuoteTable(dstTable, provider)} OFF", Array.Empty<object?>()));

        var totalStmts = batch.Count;
        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.ExecutingBatch,
            TablesTotal = tablesTotal,
            TableIndex = tableIndex,
            SourceTable = srcTable.Display,
            DestinationTable = dstTable.Display,
            CompletedStatements = 0,
            TotalStatements = totalStmts,
        });

        IProgress<(int completedStatements, int totalStatements)>? batchProgress = null;
        if (progress is not null && totalStmts > 0)
        {
            batchProgress = new Progress<(int completedStatements, int totalStatements)>(pair =>
                progress.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.ExecutingBatch,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = dstTable.Display,
                    CompletedStatements = pair.completedStatements,
                    TotalStatements = pair.totalStatements,
                }));
        }

        if (batch.Count > 0)
            await destGateway.ExecuteBatchNonQueryAsync(batch, ct, batchProgress);

        return new TableSyncResult
        {
            SourceTable = srcTable.Display,
            DestinationTable = dstTable.Display,
            Status = SyncStatus.Success,
            Inserted = inserted,
            Updated = updated,
            Deleted = deleted,
        };
    }

    private static (string Sql, IReadOnlyList<object?> Params) BuildInsert(
        Dictionary<string, object?> srcRow,
        TableRef dstTable,
        IReadOnlyList<string> srcKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valuePairs,
        TableSchema dstSchema,
        string provider)
    {
        var values = new List<(string DestCol, object? Value)>();

        // Key columns (mapped from source to destination)
        for (var k = 0; k < srcKeyColumns.Count; k++)
        {
            srcRow.TryGetValue(srcKeyColumns[k], out var keyVal);
            values.Add((destKeyColumns[k], keyVal));
        }

        // Value columns (skip identity columns on destination that are not key columns)
        var destKeySet = new HashSet<string>(destKeyColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var (sc, dc) in valuePairs)
        {
            if (destKeySet.Contains(dc)) continue;
            var destCol = dstSchema.Columns.FirstOrDefault(c => string.Equals(c.Name, dc, StringComparison.OrdinalIgnoreCase));
            if (destCol?.IsIdentity == true) continue;
            srcRow.TryGetValue(sc, out var val);
            values.Add((dc, val));
        }

        return SyncSqlBuilder.BuildInsert(dstTable, values, provider);
    }

    private static (string Sql, IReadOnlyList<object?> Params) BuildUpdate(
        Dictionary<string, object?> srcRow,
        Dictionary<string, object?> dstRow,
        TableRef dstTable,
        IReadOnlyList<string> srcKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valuePairs,
        string provider)
    {
        var setValues = new List<(string DestCol, object? Value)>();
        foreach (var (sc, dc) in valuePairs)
        {
            srcRow.TryGetValue(sc, out var val);
            setValues.Add((dc, val));
        }

        var whereValues = new List<(string DestCol, object? Value)>();
        for (var k = 0; k < destKeyColumns.Count; k++)
        {
            dstRow.TryGetValue(destKeyColumns[k], out var keyVal);
            whereValues.Add((destKeyColumns[k], keyVal));
        }

        return SyncSqlBuilder.BuildUpdate(dstTable, setValues, whereValues, provider);
    }

    private static (string Sql, IReadOnlyList<object?> Params) BuildDelete(
        Dictionary<string, object?> dstRow,
        TableRef dstTable,
        IReadOnlyList<string> destKeyColumns,
        string provider)
    {
        var whereValues = destKeyColumns.Select(k =>
        {
            dstRow.TryGetValue(k, out var val);
            return (k, val);
        }).ToList();

        return SyncSqlBuilder.BuildDelete(dstTable, whereValues, provider);
    }

    private static string KeyString(Dictionary<string, object?> row, IReadOnlyList<string> keys) =>
        string.Join('\u001F', keys.Select(k =>
            row.TryGetValue(k, out var v) ? v?.ToString() ?? "\u0000NULL" : "\u0000MISSING"));

    private static bool IsRowIncluded(SyncSelection? selection, string sourceTableDisplay, RowDifferenceKind kind,
        string keyDisplay)
    {
        if (selection?.RowsBySourceTable is null)
            return true;
        if (!selection.RowsBySourceTable.TryGetValue(sourceTableDisplay, out var set))
            return true;
        if (set.Count == 0)
            return false;
        return set.Contains(SyncSelection.FormatRowKey(kind, keyDisplay));
    }

    // --- helpers copied from DataCompareService ---

    private static List<(TableRef, TablePairSelection?)> BuildWorklist(
        CompareProject project,
        IReadOnlyList<TableRef> sourceTables,
        StringComparer nameComparer,
        List<TableSyncResult> results)
    {
        if (project.TablesToCompare.Count == 0)
            return sourceTables
                .OrderBy(t => t.Schema, nameComparer).ThenBy(t => t.Name, nameComparer)
                .Select(t => (t, (TablePairSelection?)null))
                .ToList();

        var list = new List<(TableRef, TablePairSelection?)>();
        foreach (var sel in project.TablesToCompare
                     .OrderBy(s => s.SourceSchema ?? "", nameComparer)
                     .ThenBy(s => s.SourceTable, nameComparer))
        {
            if (string.IsNullOrWhiteSpace(sel.SourceTable)) continue;
            var matches = sourceTables.Where(t =>
                nameComparer.Equals(t.Name, sel.SourceTable) &&
                (string.IsNullOrWhiteSpace(sel.SourceSchema) || nameComparer.Equals(t.Schema, sel.SourceSchema)))
                .ToList();
            if (matches.Count == 1)
            {
                list.Add((matches[0], sel));
            }
            else
            {
                var label = string.IsNullOrWhiteSpace(sel.SourceSchema) ? sel.SourceTable : $"{sel.SourceSchema}.{sel.SourceTable}";
                results.Add(new TableSyncResult
                {
                    SourceTable = label,
                    DestinationTable = "(source not found)",
                    Status = SyncStatus.Error,
                    ErrorMessage = matches.Count == 0 ? $"Source table '{label}' not found." : $"Ambiguous source table '{sel.SourceTable}'.",
                });
            }
        }

        return list;
    }

    private static Dictionary<string, TableOverride> BuildOverrideIndex(
        IEnumerable<TableOverride> overrides, StringComparer comparer) =>
        overrides
            .GroupBy(o => $"{o.SourceSchema ?? ""}\u001F{o.SourceTable}", comparer)
            .ToDictionary(g => g.Key, g => g.First(), comparer);

    private static TableOverride? FindOverride(Dictionary<string, TableOverride> index, TableRef src)
    {
        if (index.TryGetValue($"{src.Schema}\u001F{src.Name}", out var o)) return o;
        return index.TryGetValue($"\u001F{src.Name}", out o) ? o : null;
    }

    private static TableRef? ResolveDestTable(
        TableRef src, TableOverride? ov, TablePairSelection? sel,
        HashSet<TableRef> destSet, StringComparer comparer)
    {
        var destTableName = sel?.DestTable ?? ov?.DestTable;
        if (destTableName is not null)
        {
            var ds = sel?.DestSchema ?? ov?.DestSchema ?? src.Schema;
            var cand = new TableRef(ds, destTableName);
            foreach (var d in destSet)
                if (comparer.Equals(d.Schema, cand.Schema) && comparer.Equals(d.Name, cand.Name))
                    return d;
            return cand;
        }

        foreach (var k in destSet)
            if (comparer.Equals(k.Schema, src.Schema) && comparer.Equals(k.Name, src.Name))
                return k;

        return null;
    }

    private static IReadOnlyList<string>? MapSourceKeysToDestination(
        IReadOnlyList<string> sourceKeys, TableSchema dstSchema,
        Dictionary<string, string>? columnMap, StringComparer comparer)
    {
        var list = new List<string>();
        foreach (var sk in sourceKeys)
        {
            string? destName = null;
            if (columnMap is not null)
                foreach (var (s, d) in columnMap)
                    if (comparer.Equals(s, sk))
                    {
                        destName = dstSchema.Columns.Select(c => c.Name).FirstOrDefault(dc => comparer.Equals(dc, d));
                        break;
                    }
            destName ??= dstSchema.Columns.Select(c => c.Name).FirstOrDefault(dc => comparer.Equals(dc, sk));
            if (destName is null) return null;
            list.Add(destName);
        }
        return list;
    }

    private static List<(string SourceColumn, string DestinationColumn)> BuildValueColumnPairs(
        TableSchema src, TableSchema dst, TableOverride? ov,
        IReadOnlyList<string> sourceKeyColumns, StringComparer comparer)
    {
        var ignore = new HashSet<string>(ov?.IgnoreColumns ?? Enumerable.Empty<string>(), comparer);
        var map = ov?.ColumnMap;
        var keys = new HashSet<string>(sourceKeyColumns, comparer);
        var pairs = new List<(string, string)>();

        if (map is { Count: > 0 })
            foreach (var (sc, dc) in map)
            {
                if (ignore.Contains(sc) || keys.Contains(sc)) continue;
                if (!src.Columns.Any(c => comparer.Equals(c.Name, sc))) continue;
                if (!dst.Columns.Any(c => comparer.Equals(c.Name, dc))) continue;
                pairs.Add((src.Columns.First(c => comparer.Equals(c.Name, sc)).Name,
                    dst.Columns.First(c => comparer.Equals(c.Name, dc)).Name));
            }

        foreach (var scol in src.Columns)
        {
            if (ignore.Contains(scol.Name) || keys.Contains(scol.Name)) continue;
            var dcol = dst.Columns.FirstOrDefault(d => comparer.Equals(d.Name, scol.Name));
            if (dcol is null) continue;
            if (pairs.Any(p => comparer.Equals(p.Item1, scol.Name))) continue;
            pairs.Add((scol.Name, dcol.Name));
        }

        return pairs;
    }
}
