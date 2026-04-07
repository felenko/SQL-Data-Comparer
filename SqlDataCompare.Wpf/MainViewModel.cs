using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Windows;
using System.Windows.Data;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SqlDataCompare;
using SqlDataCompare.Compare;
using SqlDataCompare.Connection;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;
using SqlDataCompare.Sync;

namespace SqlDataCompare.Wpf;

public partial class MainViewModel : ObservableObject
{
    private static readonly ILogger Logger = NullLogger.Instance;

    private List<TableOverride> _persistedOverrides = new();

    [ObservableProperty] private string projectName = "New compare project";

    public EndpointEditorModel SourceEndpoint { get; } = new();
    public EndpointEditorModel DestEndpoint { get; } = new();

    public IReadOnlyList<string> EndpointKinds { get; } = EndpointEditorModel.Kinds;
    public IReadOnlyList<string> DatabaseProviders { get; } = EndpointEditorModel.DatabaseProviders;
    public IReadOnlyList<string> SqlDialects { get; } = EndpointEditorModel.SqlDialects;

    [ObservableProperty] private bool ordinalIgnoreCase = true;
    [ObservableProperty] private int maxReportedDiffsPerTable = 1000;
    [ObservableProperty] private int commandTimeoutSeconds = 120;

    [ObservableProperty] private string projectPath = "";

    public ObservableCollection<TablePairEditRow> TableRows { get; } = new();

    [ObservableProperty] private TablePairEditRow? selectedTableRow;

    public ObservableCollection<CompareResultTableVm> ResultRows { get; } = new();

    [ObservableProperty] private CompareResultTableVm? selectedCompareResult;

    [ObservableProperty] private string tableFilterText = "";

    [ObservableProperty] private string tableFilterPreset = "All";

    [ObservableProperty] private string rowDiffFilterText = "";

    [ObservableProperty] private string rowDiffKindPreset = "All kinds";

    public IReadOnlyList<string> TableFilterPresetOptions { get; } =
    [
        "All",
        "Different / sampled",
        "Not identical",
        "Identical",
        "Error",
        "Skipped",
        "Has row list",
    ];

    public IReadOnlyList<string> RowDiffKindPresetOptions { get; } =
    [
        "All kinds",
        "Insert (missing on dest)",
        "Delete (extra on dest)",
        "Update (values differ)",
    ];

    private readonly ICollectionView _filteredResultRows;

    public ICollectionView FilteredResultRows => _filteredResultRows;

    /// <summary>Filtered view over <see cref="CompareResultTableVm.RowDiffs"/> for the selected table; null when nothing selected.</summary>
    [ObservableProperty] private ICollectionView? rowDiffsView;

    public MainViewModel()
    {
        _filteredResultRows = CollectionViewSource.GetDefaultView(ResultRows);
        _filteredResultRows.Filter = CompareTableFilter;
    }

    [ObservableProperty] private string statusMessage =
        "Configure source (read-only) and destination, add tables to compare or discover them, then save or run.";

    [ObservableProperty] private bool isBusy;

    /// <summary>Determinate compare progress 0–100; meaningful while compare is running.</summary>
    [ObservableProperty] private double compareProgressPercent;

    /// <summary>True while endpoints are opening / listing tables before totals are known.</summary>
    [ObservableProperty] private bool compareProgressIndeterminate;

    [ObservableProperty] private string compareTimingText = "";

    private Stopwatch? _compareStopwatch;
    private Stopwatch? _syncStopwatch;

    [ObservableProperty] private bool syncInsertMissing = true;
    [ObservableProperty] private bool syncUpdateChanged = true;
    [ObservableProperty] private bool syncDeleteExtra = false;

    private bool CanExecuteWhenNotBusy() => !IsBusy;

    partial void OnIsBusyChanged(bool value)
    {
        RunCompareCommand.NotifyCanExecuteChanged();
        DiscoverSourceTablesCommand.NotifyCanExecuteChanged();
        SyncToDestinationCommand.NotifyCanExecuteChanged();
        TestSourceConnectionCommand.NotifyCanExecuteChanged();
        TestDestinationConnectionCommand.NotifyCanExecuteChanged();
    }

    partial void OnTableFilterTextChanged(string value) =>
        _filteredResultRows.Refresh();

    partial void OnTableFilterPresetChanged(string value) =>
        _filteredResultRows.Refresh();

    partial void OnRowDiffFilterTextChanged(string value) =>
        RowDiffsView?.Refresh();

    partial void OnRowDiffKindPresetChanged(string value) =>
        RowDiffsView?.Refresh();

    partial void OnSelectedCompareResultChanged(CompareResultTableVm? value)
    {
        if (value is null)
        {
            RowDiffsView = null;
            return;
        }

        // Use GetDefaultView(observable) — binding to CollectionViewSource.View + setting Source in code often leaves the grid empty.
        var view = CollectionViewSource.GetDefaultView(value.RowDiffs);
        view.Filter = RowDiffFilter;
        view.Refresh();
        RowDiffsView = view;
    }

    private bool CompareTableFilter(object obj)
    {
        if (obj is not CompareResultTableVm r)
            return false;

        if (!string.IsNullOrWhiteSpace(TableFilterText))
        {
            var q = TableFilterText.Trim();
            var inv = StringComparison.OrdinalIgnoreCase;
            var counts = $"{r.OnlySource} {r.OnlyDestination} {r.ValueDiffs} {r.SyncInserted} {r.SyncUpdated} {r.SyncDeleted}";
            if (!r.Source.Contains(q, inv) &&
                !r.Destination.Contains(q, inv) &&
                !r.Status.Contains(q, inv) &&
                !(r.Note?.Contains(q, inv) ?? false) &&
                !counts.Contains(q, inv))
                return false;
        }

        return TableFilterPreset switch
        {
            "Different / sampled" => r.Status is "Different" or "SampledDifferent",
            "Not identical" => r.Status != "Identical",
            "Identical" => r.Status == "Identical",
            "Error" => r.Status == "Error",
            "Skipped" => r.Status == "Skipped",
            "Has row list" => r.RowDiffs.Count > 0,
            _ => true,
        };
    }

    private bool RowDiffFilter(object obj)
    {
        if (obj is not RowDiffSelectableVm x)
            return false;

        if (!string.IsNullOrWhiteSpace(RowDiffFilterText))
        {
            var q = RowDiffFilterText.Trim();
            var inv = StringComparison.OrdinalIgnoreCase;
            var colHit = x.ColumnDiffs.Any(c =>
                c.ColumnName.Contains(q, inv) ||
                (c.SourceValue?.Contains(q, inv) ?? false) ||
                (c.DestinationValue?.Contains(q, inv) ?? false));
            if (!colHit &&
                !x.KindLabel.Contains(q, inv) &&
                !x.KeyDisplay.Contains(q, inv) &&
                !x.Detail.Contains(q, inv))
                return false;
        }

        return RowDiffKindPreset switch
        {
            "Insert (missing on dest)" => x.Kind == RowDifferenceKind.MissingInDestination,
            "Delete (extra on dest)" => x.Kind == RowDifferenceKind.MissingInSource,
            "Update (values differ)" => x.Kind == RowDifferenceKind.ValueMismatch,
            _ => true,
        };
    }

    [RelayCommand]
    private void NewProject()
    {
        ProjectPath = "";
        ProjectName = "New compare project";
        SourceEndpoint.ApplyFrom(new DatabaseEndpoint());
        DestEndpoint.ApplyFrom(new DatabaseEndpoint());
        OrdinalIgnoreCase = true;
        MaxReportedDiffsPerTable = 1000;
        CommandTimeoutSeconds = 120;
        TableRows.Clear();
        _persistedOverrides = new List<TableOverride>();
        ResultRows.Clear();
        SelectedCompareResult = null;
        TableFilterText = "";
        TableFilterPreset = "All";
        RowDiffFilterText = "";
        RowDiffKindPreset = "All kinds";
        _filteredResultRows.Refresh();
        StatusMessage = "New project. The source is never modified—only read. Any future sync would touch destination only.";
    }

    [RelayCommand]
    private void OpenProject()
    {
        var dlg = new Microsoft.Win32.OpenFileDialog
        {
            Filter = "Compare project (*.json)|*.json|All files (*.*)|*.*",
            CheckFileExists = true,
        };
        if (dlg.ShowDialog() != true)
            return;
        ProjectPath = dlg.FileName;
        LoadFromPath(ProjectPath);
    }

    private void LoadFromPath(string path)
    {
        try
        {
            var p = CompareProjectSerializer.Read(path);
            ApplyFromProject(p);
            StatusMessage = $"Loaded '{Path.GetFileName(path)}'.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed to load: {ex.Message}";
        }
    }

    private void ApplyFromProject(CompareProject p)
    {
        ProjectName = p.Name ?? "";
        SourceEndpoint.ApplyFrom(p.Source);
        DestEndpoint.ApplyFrom(p.Destination);
        OrdinalIgnoreCase = p.Options.OrdinalIgnoreCase;
        MaxReportedDiffsPerTable = p.Options.MaxReportedDiffsPerTable;
        CommandTimeoutSeconds = p.Options.CommandTimeoutSeconds;
        _persistedOverrides = p.TableOverrides.Select(o => CloneOverride(o)).ToList();

        TableRows.Clear();
        if (p.TablesToCompare.Count > 0)
        {
            foreach (var t in p.TablesToCompare)
                TableRows.Add(TablePairEditRow.FromSelection(t));
        }
        else
        {
            foreach (var o in p.TableOverrides.DistinctBy(o => $"{o.SourceSchema}\u001f{o.SourceTable}"))
                TableRows.Add(TablePairEditRow.FromOverride(o));
        }
    }

    private static TableOverride CloneOverride(TableOverride o) => new()
    {
        SourceSchema = o.SourceSchema,
        SourceTable = o.SourceTable,
        DestSchema = o.DestSchema,
        DestTable = o.DestTable,
        KeyColumns = o.KeyColumns?.ToList(),
        IgnoreColumns = o.IgnoreColumns?.ToList(),
        ColumnMap = o.ColumnMap?.ToDictionary(x => x.Key, x => x.Value),
        WhereClause = o.WhereClause,
        MaxRows = o.MaxRows,
        InsertFilePath = o.InsertFilePath,
    };

    [RelayCommand]
    private void SaveProject()
    {
        if (string.IsNullOrWhiteSpace(ProjectPath))
        {
            SaveProjectAs();
            return;
        }

        TrySave(ProjectPath);
    }

    [RelayCommand]
    private void SaveProjectAs()
    {
        var dlg = new Microsoft.Win32.SaveFileDialog
        {
            Filter = "Compare project (*.json)|*.json",
            DefaultExt = ".json",
        };
        if (dlg.ShowDialog() != true)
            return;
        ProjectPath = dlg.FileName;
        TrySave(ProjectPath);
    }

    private void TrySave(string path)
    {
        try
        {
            var p = BuildCompareProject();
            CompareProjectSerializer.Write(path, p);
            StatusMessage = $"Saved '{Path.GetFileName(path)}'.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Save failed: {ex.Message}";
        }
    }

    private CompareProject BuildCompareProject()
    {
        var tables = TableRows
            .Where(r => !string.IsNullOrWhiteSpace(r.SourceTable))
            .Select(r => r.ToSelection())
            .ToList();
        return new CompareProject
        {
            Name = string.IsNullOrWhiteSpace(ProjectName) ? null : ProjectName.Trim(),
            Source = SourceEndpoint.ToEndpoint(),
            Destination = DestEndpoint.ToEndpoint(),
            Options = new CompareOptions
            {
                OrdinalIgnoreCase = OrdinalIgnoreCase,
                TrimStrings = false,
                MaxReportedDiffsPerTable = MaxReportedDiffsPerTable <= 0 ? 1000 : MaxReportedDiffsPerTable,
                CommandTimeoutSeconds = CommandTimeoutSeconds <= 0 ? 120 : CommandTimeoutSeconds,
                DefaultLogLevel = "Information",
            },
            TablesToCompare = tables,
            TableOverrides = _persistedOverrides.Select(CloneOverride).ToList(),
        };
    }

    [RelayCommand]
    private void BrowseSqlSource()
    {
        var owner = System.Windows.Application.Current?.MainWindow;
        if (SqlServerConnectionDialog.TryShow(owner, SourceEndpoint.ConnectionString, out var cs))
        {
            SourceEndpoint.ConnectionString = cs;
            SourceEndpoint.ConnectionDpapiBase64 = null;
        }
    }

    [RelayCommand]
    private void BrowseSqlDestination()
    {
        var owner = System.Windows.Application.Current?.MainWindow;
        if (SqlServerConnectionDialog.TryShow(owner, DestEndpoint.ConnectionString, out var cs))
        {
            DestEndpoint.ConnectionString = cs;
            DestEndpoint.ConnectionDpapiBase64 = null;
        }
    }

    [RelayCommand]
    private void BrowseFolderSource()
    {
        var owner = System.Windows.Application.Current?.MainWindow;
        if (FolderPickerUi.TryPickFolder(owner, SourceEndpoint.FolderRootPath, out var path))
            SourceEndpoint.FolderRootPath = path;
    }

    [RelayCommand]
    private void BrowseFolderDestination()
    {
        var owner = System.Windows.Application.Current?.MainWindow;
        if (FolderPickerUi.TryPickFolder(owner, DestEndpoint.FolderRootPath, out var path))
            DestEndpoint.FolderRootPath = path;
    }

    private CompareOptions BuildOptionsForConnectionTest() => new()
    {
        OrdinalIgnoreCase = OrdinalIgnoreCase,
        TrimStrings = false,
        MaxReportedDiffsPerTable = MaxReportedDiffsPerTable <= 0 ? 1000 : MaxReportedDiffsPerTable,
        CommandTimeoutSeconds = CommandTimeoutSeconds <= 0 ? 120 : CommandTimeoutSeconds,
        DefaultLogLevel = "Information",
    };

    [RelayCommand(CanExecute = nameof(CanExecuteWhenNotBusy))]
    private async Task TestSourceConnectionAsync()
    {
        IsBusy = true;
        CompareProgressIndeterminate = true;
        StatusMessage = "Testing source…";
        try
        {
            var ep = SourceEndpoint.ToEndpoint();
            await EndpointConnectionTester.TestAsync(ep, BuildOptionsForConnectionTest(), CancellationToken.None);
            StatusMessage = ep is DatabaseEndpoint
                ? "Source database connection succeeded."
                : "Source folder is reachable.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Source test failed: {ex.Message}";
        }
        finally
        {
            CompareProgressIndeterminate = false;
            IsBusy = false;
        }
    }

    [RelayCommand(CanExecute = nameof(CanExecuteWhenNotBusy))]
    private async Task TestDestinationConnectionAsync()
    {
        IsBusy = true;
        CompareProgressIndeterminate = true;
        StatusMessage = "Testing destination…";
        try
        {
            var ep = DestEndpoint.ToEndpoint();
            await EndpointConnectionTester.TestAsync(ep, BuildOptionsForConnectionTest(), CancellationToken.None);
            StatusMessage = ep is DatabaseEndpoint
                ? "Destination database connection succeeded."
                : "Destination folder is reachable.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Destination test failed: {ex.Message}";
        }
        finally
        {
            CompareProgressIndeterminate = false;
            IsBusy = false;
        }
    }

    [RelayCommand]
    private void AddTableRow() =>
        TableRows.Add(new TablePairEditRow());

    [RelayCommand]
    private void RemoveTableRow()
    {
        if (SelectedTableRow is null)
            return;
        TableRows.Remove(SelectedTableRow);
        SelectedTableRow = null;
    }

    [RelayCommand(CanExecute = nameof(CanExecuteWhenNotBusy))]
    private async Task DiscoverSourceTablesAsync()
    {
        IsBusy = true;
        CompareProgressIndeterminate = true;
        StatusMessage = "Discovering tables from source…";
        try
        {
            var options = new CompareOptions
            {
                OrdinalIgnoreCase = OrdinalIgnoreCase,
                CommandTimeoutSeconds = CommandTimeoutSeconds <= 0 ? 120 : CommandTimeoutSeconds,
            };
            var ep = SourceEndpoint.ToEndpoint();
            var tables = await CompareTableDiscovery.ListTablesAsync(ep, options, CancellationToken.None);
            var comparer = OrdinalIgnoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
            foreach (var t in tables.OrderBy(x => x.Schema, comparer).ThenBy(x => x.Name, comparer))
            {
                var exists = TableRows.Any(r =>
                    comparer.Equals(r.SourceTable, t.Name) &&
                    (string.IsNullOrWhiteSpace(r.SourceSchema) || comparer.Equals(r.SourceSchema, t.Schema)));
                if (!exists)
                    TableRows.Add(new TablePairEditRow { SourceSchema = t.Schema, SourceTable = t.Name });
            }

            StatusMessage = $"Discovered {tables.Count} table(s) from source; added missing rows. Source is read-only.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Discovery failed: {ex.Message}";
        }
        finally
        {
            CompareProgressIndeterminate = false;
            IsBusy = false;
        }
    }

    [RelayCommand(CanExecute = nameof(CanExecuteWhenNotBusy))]
    private async Task RunCompareAsync()
    {
        ResultRows.Clear();
        SelectedCompareResult = null;
        TableFilterPreset = "All";
        RowDiffKindPreset = "All kinds";
        _filteredResultRows.Refresh();
        CompareProject project;
        try
        {
            project = BuildCompareProject();
        }
        catch (Exception ex)
        {
            StatusMessage = $"Invalid project: {ex.Message}";
            return;
        }

        if (project.Source is DatabaseEndpoint sdb && string.IsNullOrWhiteSpace(sdb.ConnectionString) &&
            string.IsNullOrWhiteSpace(sdb.ConnectionStringDpapiBase64))
        {
            StatusMessage = "Set a source connection string or choose an INSERT folder.";
            return;
        }

        if (project.Source is InsertFolderEndpoint sfo && string.IsNullOrWhiteSpace(sfo.RootPath))
        {
            StatusMessage = "Set the source folder path for INSERT scripts.";
            return;
        }

        if (project.Destination is DatabaseEndpoint ddb && string.IsNullOrWhiteSpace(ddb.ConnectionString) &&
            string.IsNullOrWhiteSpace(ddb.ConnectionStringDpapiBase64))
        {
            StatusMessage = "Set a destination connection string or choose an INSERT folder.";
            return;
        }

        if (project.Destination is InsertFolderEndpoint dfo && string.IsNullOrWhiteSpace(dfo.RootPath))
        {
            StatusMessage = "Set the destination folder path for INSERT scripts.";
            return;
        }

        IsBusy = true;
        CompareProgressIndeterminate = true;
        CompareProgressPercent = 0;
        CompareTimingText = "";
        StatusMessage = "Connecting and listing tables…";
        _compareStopwatch = Stopwatch.StartNew();
        var progress = new Progress<CompareProgressInfo>(OnCompareProgress);
        try
        {
            var svc = new DataCompareService();
            var result = await svc.RunAsync(project, Logger, CancellationToken.None, progress);

            var diff = result.Tables.Count(x =>
                x.Status is TableCompareStatus.Different or TableCompareStatus.SampledDifferent);
            var err = result.Tables.Count(x => x.Status == TableCompareStatus.Error);
            var elapsedStr = FormatShortTime(_compareStopwatch.Elapsed);
            StatusMessage =
                $"Done in {elapsedStr}. Identical: {result.Tables.Count(x => x.Status == TableCompareStatus.Identical)}, different: {diff}, errors: {err}, skipped: {result.Tables.Count(x => x.Status == TableCompareStatus.Skipped)}.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Failed: {ex.Message}";
        }
        finally
        {
            _compareStopwatch?.Stop();
            _compareStopwatch = null;
            IsBusy = false;
            CompareProgressIndeterminate = false;
            CompareProgressPercent = 0;
            CompareTimingText = "";
        }
    }

    private void OnCompareProgress(CompareProgressInfo p)
    {
        if (p.LatestTable is { } t)
            ResultRows.Add(CompareResultTableVm.From(t));

        if (p.TotalTables > 0)
        {
            CompareProgressIndeterminate = false;
            CompareProgressPercent = Math.Min(100, p.CompletedTables * 100.0 / p.TotalTables);
        }
        else
        {
            CompareProgressIndeterminate = false;
            CompareProgressPercent = 0;
        }

        var elapsed = _compareStopwatch?.Elapsed ?? TimeSpan.Zero;
        CompareTimingText = FormatCompareTimingLine(elapsed, p.CompletedTables, p.TotalTables);

        if (p.TotalTables > 0)
        {
            if (p.LatestTable is null && p.CompletedTables == 0)
                StatusMessage = $"Preparing {p.TotalTables} table(s) (source is read-only)…";
            else
            {
                var name = p.LatestTable?.SourceTable ?? "…";
                StatusMessage = $"Comparing {p.CompletedTables}/{p.TotalTables} — {name}";
            }
        }
        else if (p is { CompletedTables: 0, TotalTables: 0 })
        {
            StatusMessage = "No tables to compare.";
        }
    }

    private static string FormatShortTime(TimeSpan t)
    {
        if (t.TotalHours >= 1)
            return $"{(int)t.TotalHours}:{t.Minutes:00}:{t.Seconds:00}";
        if (t.TotalMinutes >= 1)
            return $"{t.Minutes}:{t.Seconds:00}";
        return $"{t.Seconds}s";
    }

    private static string FormatCompareTimingLine(TimeSpan elapsed, int completed, int total)
    {
        var el = FormatShortTime(elapsed);
        if (total <= 0 || completed >= total)
            return $"Elapsed {el}";
        if (completed <= 0)
            return $"Elapsed {el}";
        var rate = elapsed.TotalSeconds / completed;
        var remaining = TimeSpan.FromSeconds(rate * (total - completed));
        return $"Elapsed {el} · ~{FormatShortTime(remaining)} left";
    }

    [RelayCommand(CanExecute = nameof(CanExecuteWhenNotBusy))]
    private async Task SyncToDestinationAsync()
    {
        CompareProject project;
        try
        {
            project = BuildCompareProject();
        }
        catch (Exception ex)
        {
            StatusMessage = $"Invalid project: {ex.Message}";
            return;
        }

        if (project.Destination is not DatabaseEndpoint)
        {
            StatusMessage = "Sync requires a database destination (not an INSERT folder).";
            return;
        }

        var actions = new List<string>();
        if (SyncInsertMissing) actions.Add("insert rows missing in destination");
        if (SyncUpdateChanged) actions.Add("update rows with changed values");
        if (SyncDeleteExtra) actions.Add("DELETE rows not in source");
        if (actions.Count == 0)
        {
            StatusMessage = "No sync actions selected. Enable at least one option.";
            return;
        }

        var msg = $"This will modify the DESTINATION database:\n\n• {string.Join("\n• ", actions)}\n\nThe source is never modified. Proceed?";
        if (System.Windows.MessageBox.Show(msg, "Confirm sync to destination",
                MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes)
            return;

        if (!ResultRows.Any(r => r.IncludeInSync))
        {
            StatusMessage = "Select at least one table for sync (check Sync in the Results grid).";
            return;
        }

        IsBusy = true;
        CompareProgressIndeterminate = true;
        CompareProgressPercent = 0;
        CompareTimingText = "";
        StatusMessage = "Starting sync…";
        _syncStopwatch = Stopwatch.StartNew();
        var syncProgress = new Progress<SyncProgressInfo>(OnSyncProgress);
        try
        {
            var syncOptions = new SyncOptions
            {
                InsertMissing = SyncInsertMissing,
                UpdateChanged = SyncUpdateChanged,
                DeleteExtra = SyncDeleteExtra,
                Selection = BuildSyncSelection(ResultRows),
            };
            var svc = new DataSyncService();
            var result = await svc.RunAsync(project, syncOptions, Logger, CancellationToken.None, syncProgress);

            var syncIndex = result.Tables.ToDictionary(t => t.SourceTable, StringComparer.OrdinalIgnoreCase);
            foreach (var row in ResultRows)
            {
                if (syncIndex.TryGetValue(row.Source, out var sr))
                    row.ApplySync(sr);
            }

            var totalInserted = result.Tables.Sum(t => t.Inserted);
            var totalUpdated = result.Tables.Sum(t => t.Updated);
            var totalDeleted = result.Tables.Sum(t => t.Deleted);
            var errors = result.Tables.Count(t => t.Status == SyncStatus.Error);
            var elapsed = FormatShortTime(_syncStopwatch.Elapsed);
            CompareProgressPercent = 100;
            CompareProgressIndeterminate = false;
            StatusMessage = $"Sync done in {elapsed}. Inserted: {totalInserted}, updated: {totalUpdated}, deleted: {totalDeleted}" +
                            (errors > 0 ? $", errors: {errors}" : "") + ".";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Sync failed: {ex.Message}";
        }
        finally
        {
            _syncStopwatch?.Stop();
            _syncStopwatch = null;
            CompareProgressIndeterminate = false;
            CompareProgressPercent = 0;
            CompareTimingText = "";
            IsBusy = false;
        }
    }

    private void OnSyncProgress(SyncProgressInfo p)
    {
        var elapsed = _syncStopwatch?.Elapsed ?? TimeSpan.Zero;
        CompareTimingText = $"Elapsed {FormatShortTime(elapsed)}";

        switch (p.Phase)
        {
            case SyncProgressPhase.Started:
                CompareProgressIndeterminate = p.TablesTotal == 0;
                CompareProgressPercent = 0;
                StatusMessage = p.TablesTotal == 0
                    ? "Sync: no tables in worklist."
                    : $"Sync: {p.TablesTotal} table(s) to process…";
                break;

            case SyncProgressPhase.TableStarting:
                CompareProgressIndeterminate = true;
                CompareProgressPercent = p.TablesTotal > 0
                    ? Math.Min(99, (p.TableIndex - 1) * 100.0 / p.TablesTotal)
                    : 0;
                StatusMessage =
                    $"Sync {p.TableIndex}/{Math.Max(1, p.TablesTotal)}: {p.SourceTable} → {p.DestinationTable} — preparing…";
                break;

            case SyncProgressPhase.LoadingRows:
                CompareProgressIndeterminate = true;
                StatusMessage =
                    $"Sync {p.TableIndex}/{Math.Max(1, p.TablesTotal)}: {p.SourceTable} — reading rows from source & destination…";
                break;

            case SyncProgressPhase.ExecutingBatch:
                CompareProgressIndeterminate = false;
                var tw = p.TablesTotal > 0 ? p.TablesTotal : 1;
                var tableSpan = 100.0 / tw;
                var basePct = (p.TableIndex - 1) * tableSpan;
                var stmtFrac = p.TotalStatements > 0 ? p.CompletedStatements / (double)p.TotalStatements : 1;
                CompareProgressPercent = Math.Min(99.9, basePct + stmtFrac * tableSpan);
                var stmtLabel = p.TotalStatements > 0
                    ? $"command {p.CompletedStatements}/{p.TotalStatements}"
                    : "no SQL commands";
                StatusMessage =
                    $"Sync {p.TableIndex}/{Math.Max(1, p.TablesTotal)}: {p.SourceTable} — applying ({stmtLabel})…";
                break;

            case SyncProgressPhase.TableCompleted:
                if (p.TablesTotal > 0)
                    CompareProgressPercent = Math.Min(100, p.TableIndex * 100.0 / p.TablesTotal);
                break;

            case SyncProgressPhase.Finished:
                CompareProgressPercent = 100;
                CompareProgressIndeterminate = false;
                break;
        }
    }

    private static SyncSelection? BuildSyncSelection(IEnumerable<CompareResultTableVm> rows)
    {
        var list = rows.ToList();
        HashSet<string>? includedTables = null;
        if (list.Any(r => !r.IncludeInSync))
            includedTables = list.Where(r => r.IncludeInSync).Select(r => r.Source).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Dictionary<string, HashSet<string>>? rowFilters = null;
        foreach (var t in list.Where(r => r.IncludeInSync))
        {
            if (t.RowDiffs.Count == 0)
                continue;
            if (t.RowDiffs.All(x => x.IncludeInSync))
                continue;

            rowFilters ??= new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var keys = t.RowDiffs.Where(x => x.IncludeInSync).Select(x => x.SelectionKey).ToHashSet(StringComparer.Ordinal);
            rowFilters[t.Source] = keys;
        }

        if (includedTables is null && rowFilters is null)
            return null;

        return new SyncSelection
        {
            IncludedSourceTables = includedTables,
            RowsBySourceTable = rowFilters,
        };
    }

    [RelayCommand]
    private void SelectAllTablesForSync()
    {
        foreach (var r in ResultRows)
            r.IncludeInSync = true;
    }

    [RelayCommand]
    private void ClearTablesForSync()
    {
        foreach (var r in ResultRows)
            r.IncludeInSync = false;
    }

    [RelayCommand]
    private void SelectAllRowDiffs()
    {
        if (SelectedCompareResult is null)
            return;
        foreach (var x in SelectedCompareResult.RowDiffs)
            x.IncludeInSync = true;
    }

    [RelayCommand]
    private void ClearRowDiffs()
    {
        if (SelectedCompareResult is null)
            return;
        foreach (var x in SelectedCompareResult.RowDiffs)
            x.IncludeInSync = false;
    }

    [RelayCommand]
    private void ClearTableFilters()
    {
        TableFilterText = "";
        TableFilterPreset = "All";
        _filteredResultRows.Refresh();
    }

    [RelayCommand]
    private void ClearRowDiffFilters()
    {
        RowDiffFilterText = "";
        RowDiffKindPreset = "All kinds";
        RowDiffsView?.Refresh();
    }
}
