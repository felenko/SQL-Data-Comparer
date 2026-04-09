using Microsoft.Extensions.Logging;
using SqlDataCompare.Compare;
using SqlDataCompare.DataSources;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;
using SqlDataCompare.Sql;

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

        if (project.Destination is InsertFolderEndpoint folderDest)
            return await SyncToFolderAsync(project, folderDest, syncOptions, logger, cancellationToken, progress);

        if (project.Destination is not DatabaseEndpoint destDbEndpoint)
            throw new InvalidOperationException("Unsupported destination endpoint type.");

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

        // Topological sort: resolve destination tables, load FK graph, reorder worklist
        // so that parent (referenced) tables are synced before child (referencing) tables.
        try
        {
            var destTableMapping = work.Select(w =>
            {
                var ov = FindOverride(overrideIndex, w.Item1);
                return ResolveDestTable(w.Item1, ov, w.Item2, destSet, nameComparer);
            }).ToList();

            var resolvedDestTables = destTableMapping
                .Where(d => d.HasValue).Select(d => d!.Value)
                .Distinct(TableRefEqualityComparer.OrdinalIgnoreCase).ToList();

            if (resolvedDestTables.Count > 0)
            {
                var fkEdges = await ForeignKeyLoader.LoadAsync(
                    destDbEndpoint, project.Options, resolvedDestTables, cancellationToken);

                if (fkEdges.Count > 0)
                {
                    var (sorted, cycles) = TableSyncOrderer.Sort(resolvedDestTables, fkEdges);

                    if (cycles.Count > 0)
                        logger.LogWarning(
                            "Circular FK references detected among tables [{Tables}]. " +
                            "Enable 'Disable FK Checks' for reliable sync.",
                            string.Join(", ", cycles.Select(t => t.Display)));

                    var sortedPos = new Dictionary<TableRef, int>(TableRefEqualityComparer.OrdinalIgnoreCase);
                    for (var s = 0; s < sorted.Count; s++)
                        sortedPos[sorted[s]] = s;

                    work = work
                        .Zip(destTableMapping, (w, d) => (Src: w.Item1, Sel: w.Item2, Dest: d))
                        .OrderBy(x => x.Dest.HasValue && sortedPos.TryGetValue(x.Dest.Value, out var pos) ? pos : int.MaxValue)
                        .Select(x => (x.Src, x.Sel))
                        .ToList();
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "FK dependency analysis failed; syncing tables in original order.");
        }

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
            if (ov?.SkipCompare == true)
            {
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableStarting,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = "(skipped)",
                });
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = "(skipped)",
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "Skipped at setup (table override).",
                });
                progress?.Report(new SyncProgressInfo
                {
                    Phase = SyncProgressPhase.TableCompleted,
                    TablesTotal = tablesTotal,
                    TableIndex = tableIndex,
                    SourceTable = srcTable.Display,
                    DestinationTable = "(skipped)",
                });
                continue;
            }

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
                    project.Options.SkipBinaryColumnsInCompare,
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
        bool skipBinaryColumnsInCompare,
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

        var valuePairs = CompareColumnPairBuilder.BuildValueColumnPairs(
            srcSchema, dstSchema, ov, keys.KeyColumns, nameComparer, skipBinaryColumnsInCompare);

        var where = srcEndpoint is DatabaseEndpoint ? ov?.WhereClause : null;
        var destWhere = dstEndpoint is DatabaseEndpoint ? ov?.WhereClause : null;

        var srcProject = keys.KeyColumns.Concat(valuePairs.Select(p => p.SourceColumn))
            .Distinct(StringComparer.Ordinal).ToList();
        var dstProject = destKeyColumns.Concat(valuePairs.Select(p => p.DestinationColumn))
            .Distinct(StringComparer.Ordinal).ToList();

        var srcTask = sourceSide.LoadRowsAsync(srcTable, ov, srcProject, keys.KeyColumns, where, null, ct);
        var dstTask = destSide.LoadRowsAsync(dstTable, ov, dstProject, destKeyColumns, destWhere, null, ct);
        await Task.WhenAll(srcTask, dstTask);
        var srcRows = await srcTask;
        var dstRows = await dstTask;

        // Same ordering as compare: reuse fast path when SQL already returned rows ordered by key.
        var srcSorted = RowMergeComparer.EnsureSortedByKeyString(srcRows, keys.KeyColumns, ct);
        var dstSorted = RowMergeComparer.EnsureSortedByKeyString(dstRows, destKeyColumns, ct);

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
                        if (BuildUpdate(srcSorted[i], dstSorted[j], dstTable, keys.KeyColumns, destKeyColumns, valuePairs,
                                dstSchema, provider) is { } updStmt)
                        {
                            batch.Add(updStmt);
                            updated++;
                        }
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

        // Wrap with FK-check suppression when requested.
        // Must be in the same ExecuteBatchNonQueryAsync call because MySQL/PostgreSQL
        // disable is connection-scoped; SQL Server uses ALTER TABLE which is persistent.
        if (opts.DisableForeignKeyChecks && batch.Count > 0)
        {
            var (disableFkSql, enableFkSql) = SyncSqlBuilder.GetFkCheckWrapSql(dstTable, provider);
            batch.Insert(0, (disableFkSql, Array.Empty<object?>()));
            batch.Add((enableFkSql, Array.Empty<object?>()));
        }

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
            if (SyncBinaryColumnGuard.ShouldOmitValue(destCol?.PhysicalType, val))
                continue;
            values.Add((dc, val));
        }

        return SyncSqlBuilder.BuildInsert(dstTable, values, provider);
    }

    private static (string Sql, IReadOnlyList<object?> Params)? BuildUpdate(
        Dictionary<string, object?> srcRow,
        Dictionary<string, object?> dstRow,
        TableRef dstTable,
        IReadOnlyList<string> srcKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valuePairs,
        TableSchema dstSchema,
        string provider)
    {
        var setValues = new List<(string DestCol, object? Value)>();
        foreach (var (sc, dc) in valuePairs)
        {
            srcRow.TryGetValue(sc, out var val);
            var destCol = dstSchema.Columns.FirstOrDefault(c => string.Equals(c.Name, dc, StringComparison.OrdinalIgnoreCase));
            if (SyncBinaryColumnGuard.ShouldOmitValue(destCol?.PhysicalType, val))
                continue;
            setValues.Add((dc, val));
        }

        if (setValues.Count == 0)
            return null;

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

    // --- folder (INSERT file) sync ---

    private static async Task<ProjectSyncResult> SyncToFolderAsync(
        CompareProject project,
        InsertFolderEndpoint folderDest,
        SyncOptions syncOptions,
        ILogger logger,
        CancellationToken cancellationToken,
        IProgress<SyncProgressInfo>? progress)
    {
        var nameComparer = project.Options.OrdinalIgnoreCase
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;
        var valueComparer = new ValueComparer(project.Options.OrdinalIgnoreCase, project.Options.TrimStrings);

        var results = new List<TableSyncResult>();

        await using var sourceSide = CompareSide.Create(project.Source, project.Options, isSource: true);
        await using var destSide = CompareSide.Create(project.Destination, project.Options, isSource: false);

        var sourceTables = await sourceSide.ListTablesAsync(cancellationToken);
        var destTables = await destSide.ListTablesAsync(cancellationToken);
        var destSet = destTables.ToHashSet(TableRefComparer.Instance);

        var overrideIndex = BuildOverrideIndex(project.TableOverrides, nameComparer);
        var work = BuildWorklist(project, sourceTables, nameComparer, results);

        var dialect = InsertSqlDialectParser.Parse(folderDest.SqlDialect);
        var rootPath = Path.GetFullPath(folderDest.RootPath);
        Directory.CreateDirectory(rootPath);

        var tablesTotal = work.Count;
        progress?.Report(new SyncProgressInfo { Phase = SyncProgressPhase.Started, TablesTotal = tablesTotal, TableIndex = 0 });

        var tableIndex = 0;
        foreach (var (srcTable, sel) in work)
        {
            cancellationToken.ThrowIfCancellationRequested();
            tableIndex++;
            var ov = FindOverride(overrideIndex, srcTable);

            if (ov?.SkipCompare == true)
            {
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = "(skipped)",
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "Skipped at setup (table override).",
                });
                continue;
            }

            // Resolve dest table; fall back to same name even if folder file doesn't exist yet
            var resolvedDest = ResolveDestTable(srcTable, ov, sel, destSet, nameComparer);
            var destTable = resolvedDest ?? new TableRef(
                sel?.DestSchema ?? ov?.DestSchema ?? srcTable.Schema,
                sel?.DestTable ?? ov?.DestTable ?? srcTable.Name);

            if (syncOptions.Selection?.IncludedSourceTables is { Count: > 0 } included &&
                !included.Contains(srcTable.Display))
            {
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = destTable.Display,
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "Table not selected for sync.",
                });
                continue;
            }

            progress?.Report(new SyncProgressInfo
            {
                Phase = SyncProgressPhase.TableStarting,
                TablesTotal = tablesTotal,
                TableIndex = tableIndex,
                SourceTable = srcTable.Display,
                DestinationTable = destTable.Display,
            });

            try
            {
                var syncResult = await CopyTableToFolderAsync(
                    srcTable, destTable, ov,
                    sourceSide, destSide,
                    project.Source,
                    nameComparer, valueComparer,
                    dialect, rootPath, folderDest.FileNaming,
                    syncOptions, logger,
                    project.Options.SkipBinaryColumnsInCompare,
                    tableIndex, tablesTotal, progress,
                    cancellationToken);
                results.Add(syncResult);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed copying {Src} to folder", srcTable.Display);
                results.Add(new TableSyncResult
                {
                    SourceTable = srcTable.Display,
                    DestinationTable = destTable.Display,
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
                DestinationTable = destTable.Display,
            });
        }

        progress?.Report(new SyncProgressInfo { Phase = SyncProgressPhase.Finished, TablesTotal = tablesTotal, TableIndex = tablesTotal });
        return new ProjectSyncResult { Tables = results };
    }

    private static async Task<TableSyncResult> CopyTableToFolderAsync(
        TableRef srcTable,
        TableRef destTable,
        TableOverride? ov,
        CompareSide sourceSide,
        CompareSide destSide,
        DataEndpoint srcEndpoint,
        StringComparer nameComparer,
        ValueComparer valueComparer,
        InsertSqlDialect dialect,
        string rootPath,
        string fileNaming,
        SyncOptions opts,
        ILogger logger,
        bool skipBinaryColumns,
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
            DestinationTable = destTable.Display,
        });

        var srcSchema = await sourceSide.GetSchemaAsync(srcTable, ov, ct);
        if (srcSchema is null)
            throw new InvalidOperationException("Could not load source table schema.");

        var dstSchema = await destSide.GetSchemaAsync(destTable, ov, ct);
        // Folder file doesn't exist yet — mirror source schema so key mapping works
        if (dstSchema is null || dstSchema.Columns.Count == 0)
            dstSchema = srcSchema;

        var keys = PrimaryKeyResolver.Resolve(srcSchema, ov, nameComparer);
        if (keys.KeyColumns.Count == 0 || keys.Confidence == KeyConfidence.Ambiguous)
            return new TableSyncResult
            {
                SourceTable = srcTable.Display,
                DestinationTable = destTable.Display,
                Status = SyncStatus.Skipped,
                ErrorMessage = keys.Detail ?? "Key columns could not be resolved.",
            };

        var destKeyColumns = MapSourceKeysToDestination(keys.KeyColumns, dstSchema, ov?.ColumnMap, nameComparer);
        if (destKeyColumns is null)
            throw new InvalidOperationException("Could not map key columns to destination.");

        var valuePairs = CompareColumnPairBuilder.BuildValueColumnPairs(
            srcSchema, dstSchema, ov, keys.KeyColumns, nameComparer, skipBinaryColumns);

        var where = srcEndpoint is DatabaseEndpoint ? ov?.WhereClause : null;
        var srcProject = keys.KeyColumns.Concat(valuePairs.Select(p => p.SourceColumn)).Distinct(StringComparer.Ordinal).ToList();
        var dstProject = destKeyColumns.Concat(valuePairs.Select(p => p.DestinationColumn)).Distinct(StringComparer.Ordinal).ToList();

        var srcTask = sourceSide.LoadRowsAsync(srcTable, ov, srcProject, keys.KeyColumns, where, ov?.MaxRows, ct);
        var dstTask = destSide.LoadRowsAsync(destTable, ov, dstProject, destKeyColumns, null, ov?.MaxRows, ct);
        await Task.WhenAll(srcTask, dstTask);
        var srcRows = await srcTask;
        var dstRows = await dstTask;

        var srcSorted = RowMergeComparer.EnsureSortedByKeyString(srcRows, keys.KeyColumns, ct);
        var dstSorted = RowMergeComparer.EnsureSortedByKeyString(dstRows, destKeyColumns, ct);

        var lines = new List<string>();
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
                    lines.Add(BuildLiteralInsert(srcSorted[i], destTable, keys.KeyColumns, destKeyColumns, valuePairs, dstSchema, dialect));
                    inserted++;
                }
                i++;
            }
            else if (cmp > 0)
            {
                if (opts.DeleteExtra &&
                    IsRowIncluded(opts.Selection, srcTable.Display, RowDifferenceKind.MissingInSource, kd))
                {
                    lines.Add(BuildLiteralDelete(dstSorted[j], destTable, destKeyColumns, dialect));
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
                        lines.Add(BuildLiteralUpdate(srcSorted[i], destTable, keys.KeyColumns, destKeyColumns, valuePairs, dstSchema, dialect));
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
                lines.Add(BuildLiteralInsert(srcSorted[i], destTable, keys.KeyColumns, destKeyColumns, valuePairs, dstSchema, dialect));
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
                lines.Add(BuildLiteralDelete(dstSorted[j], destTable, destKeyColumns, dialect));
                deleted++;
            }
            j++;
        }

        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.ExecutingBatch,
            TablesTotal = tablesTotal,
            TableIndex = tableIndex,
            SourceTable = srcTable.Display,
            DestinationTable = destTable.Display,
            CompletedStatements = 0,
            TotalStatements = lines.Count,
        });

        if (lines.Count > 0)
        {
            var fileName = InsertFolderNaming.FileNameForTable(destTable, fileNaming);
            var filePath = Path.Combine(rootPath, fileName);
            await File.WriteAllLinesAsync(filePath, lines, ct);
            logger.LogInformation(
                "Wrote {Count} statement(s) to {File} (inserts={I}, updates={U}, deletes={D})",
                lines.Count, fileName, inserted, updated, deleted);
        }

        progress?.Report(new SyncProgressInfo
        {
            Phase = SyncProgressPhase.ExecutingBatch,
            TablesTotal = tablesTotal,
            TableIndex = tableIndex,
            SourceTable = srcTable.Display,
            DestinationTable = destTable.Display,
            CompletedStatements = lines.Count,
            TotalStatements = lines.Count,
        });

        return new TableSyncResult
        {
            SourceTable = srcTable.Display,
            DestinationTable = destTable.Display,
            Status = SyncStatus.Success,
            Inserted = inserted,
            Updated = updated,
            Deleted = deleted,
        };
    }

    private static string BuildLiteralInsert(
        Dictionary<string, object?> srcRow,
        TableRef destTable,
        IReadOnlyList<string> srcKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valuePairs,
        TableSchema dstSchema,
        InsertSqlDialect dialect)
    {
        var values = new List<(string DestCol, object? Value)>();
        for (var k = 0; k < srcKeyColumns.Count; k++)
        {
            srcRow.TryGetValue(srcKeyColumns[k], out var v);
            values.Add((destKeyColumns[k], v));
        }
        var destKeySet = new HashSet<string>(destKeyColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var (sc, dc) in valuePairs)
        {
            if (destKeySet.Contains(dc)) continue;
            var col = dstSchema.Columns.FirstOrDefault(c => string.Equals(c.Name, dc, StringComparison.OrdinalIgnoreCase));
            if (col?.IsIdentity == true) continue;
            srcRow.TryGetValue(sc, out var val);
            values.Add((dc, val));
        }
        var qt = SyncSqlBuilder.QuoteTableForDialect(destTable, dialect);
        var cols = string.Join(", ", values.Select(v => SyncSqlBuilder.QuoteColumnForDialect(v.DestCol, dialect)));
        var vals = string.Join(", ", values.Select(v => SyncSqlBuilder.FormatLiteralValue(v.Value, dialect)));
        return $"INSERT INTO {qt} ({cols}) VALUES ({vals});";
    }

    private static string BuildLiteralUpdate(
        Dictionary<string, object?> srcRow,
        TableRef destTable,
        IReadOnlyList<string> srcKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valuePairs,
        TableSchema dstSchema,
        InsertSqlDialect dialect)
    {
        var setParts = new List<string>();
        foreach (var (sc, dc) in valuePairs)
        {
            srcRow.TryGetValue(sc, out var val);
            var col = dstSchema.Columns.FirstOrDefault(c => string.Equals(c.Name, dc, StringComparison.OrdinalIgnoreCase));
            if (SyncBinaryColumnGuard.ShouldOmitValue(col?.PhysicalType, val)) continue;
            setParts.Add($"{SyncSqlBuilder.QuoteColumnForDialect(dc, dialect)} = {SyncSqlBuilder.FormatLiteralValue(val, dialect)}");
        }
        var whereParts = new List<string>();
        for (var k = 0; k < srcKeyColumns.Count; k++)
        {
            srcRow.TryGetValue(srcKeyColumns[k], out var v);
            whereParts.Add($"{SyncSqlBuilder.QuoteColumnForDialect(destKeyColumns[k], dialect)} = {SyncSqlBuilder.FormatLiteralValue(v, dialect)}");
        }
        var qt = SyncSqlBuilder.QuoteTableForDialect(destTable, dialect);
        return $"UPDATE {qt} SET {string.Join(", ", setParts)} WHERE {string.Join(" AND ", whereParts)};";
    }

    private static string BuildLiteralDelete(
        Dictionary<string, object?> dstRow,
        TableRef destTable,
        IReadOnlyList<string> destKeyColumns,
        InsertSqlDialect dialect)
    {
        var whereParts = destKeyColumns.Select(k =>
        {
            dstRow.TryGetValue(k, out var v);
            return $"{SyncSqlBuilder.QuoteColumnForDialect(k, dialect)} = {SyncSqlBuilder.FormatLiteralValue(v, dialect)}";
        });
        var qt = SyncSqlBuilder.QuoteTableForDialect(destTable, dialect);
        return $"DELETE FROM {qt} WHERE {string.Join(" AND ", whereParts)};";
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
            if (sel.Skip)
            {
                var skipLabel = string.IsNullOrWhiteSpace(sel.SourceSchema)
                    ? sel.SourceTable.Trim()
                    : $"{sel.SourceSchema}.{sel.SourceTable}";
                results.Add(new TableSyncResult
                {
                    SourceTable = skipLabel,
                    DestinationTable = "(skipped)",
                    Status = SyncStatus.Skipped,
                    ErrorMessage = "Skipped at setup.",
                });
                continue;
            }

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

}
