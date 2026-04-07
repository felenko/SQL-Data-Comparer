using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using SqlDataCompare.Compare;
using SqlDataCompare.Sync;

namespace SqlDataCompare.Wpf;

public partial class CompareResultTableVm : ObservableObject
{
    public CompareResultTableVm(
        string source,
        string destination,
        string status,
        long onlySource,
        long onlyDestination,
        long valueDiffs,
        string? initialNote,
        bool rowListTruncated,
        string? truncationHint)
    {
        Source = source;
        Destination = destination;
        Status = status;
        OnlySource = onlySource;
        OnlyDestination = onlyDestination;
        ValueDiffs = valueDiffs;
        Note = initialNote;
        RowListTruncated = rowListTruncated;
        TruncationHint = truncationHint;
    }

    public string Source { get; }
    public string Destination { get; }
    public string Status { get; private set; }
    public long OnlySource { get; }
    public long OnlyDestination { get; }
    public long ValueDiffs { get; }

    [ObservableProperty] private string? note;
    public bool RowListTruncated { get; }
    public string? TruncationHint { get; }

    [ObservableProperty] private bool includeInSync;

    [ObservableProperty] private long? syncInserted;
    [ObservableProperty] private long? syncUpdated;
    [ObservableProperty] private long? syncDeleted;

    public ObservableCollection<RowDiffSelectableVm> RowDiffs { get; } = new();

    /// <summary>True when compare counts show differences but no sample rows were materialized (unexpected; check max-diffs).</summary>
    public bool HasCountsButNoRowSample =>
        RowDiffs.Count == 0 && (OnlySource + OnlyDestination + ValueDiffs) > 0 &&
        Status is nameof(TableCompareStatus.Different) or nameof(TableCompareStatus.SampledDifferent);

    public static CompareResultTableVm From(TablePairCompareResult t)
    {
        var totalDiffApprox = t.RowsOnlyInSource + t.RowsOnlyInDestination + t.RowsWithValueDifferences;
        var truncated = t.Sampled || (totalDiffApprox > t.SampleDiffs.Count && t.SampleDiffs.Count > 0);
        var hint = truncated
            ? "Not all differing rows are listed. Increase Max reported diffs / table in Compare options and run compare again to select more rows."
            : null;

        var vm = new CompareResultTableVm(
            t.SourceTable,
            t.DestinationTable,
            t.Status.ToString(),
            t.RowsOnlyInSource,
            t.RowsOnlyInDestination,
            t.RowsWithValueDifferences,
            t.ErrorMessage,
            truncated,
            hint)
        {
            IncludeInSync = t.Status is TableCompareStatus.Different or TableCompareStatus.SampledDifferent,
        };

        foreach (var d in t.SampleDiffs)
            vm.RowDiffs.Add(RowDiffSelectableVm.From(d));

        return vm;
    }

    public void ApplySync(TableSyncResult s)
    {
        SyncInserted = s.Inserted;
        SyncUpdated = s.Updated;
        SyncDeleted = s.Deleted;
        if (s.Status == SyncStatus.Error && !string.IsNullOrEmpty(s.ErrorMessage))
            Note = s.ErrorMessage;
    }
}

/// <summary>One differing column for display in the row details grid (value-compare rows).</summary>
public sealed class ColumnDiffRowVm
{
    public string ColumnName { get; }
    public string? SourceValue { get; }
    public string? DestinationValue { get; }

    public ColumnDiffRowVm(string columnName, string? sourceValue, string? destinationValue)
    {
        ColumnName = columnName;
        SourceValue = sourceValue;
        DestinationValue = destinationValue;
    }
}

public partial class RowDiffSelectableVm : ObservableObject
{
    public RowDiffSelectableVm(RowDifferenceKind kind, string keyDisplay, string detail)
    {
        Kind = kind;
        KeyDisplay = keyDisplay;
        Detail = detail;
    }

    public RowDifferenceKind Kind { get; }
    public string KeyDisplay { get; }
    public string Detail { get; }

    public ObservableCollection<ColumnDiffRowVm> ColumnDiffs { get; } = new();

    [ObservableProperty] private bool includeInSync = true;

    public string KindLabel => Kind switch
    {
        RowDifferenceKind.MissingInDestination => "Insert (missing on dest)",
        RowDifferenceKind.MissingInSource => "Delete (extra on dest)",
        RowDifferenceKind.ValueMismatch => "Update (values differ)",
        _ => Kind.ToString(),
    };

    public string SelectionKey => SyncSelection.FormatRowKey(Kind, KeyDisplay);

    public static RowDiffSelectableVm From(RowDifference d)
    {
        var detail = d.ColumnMismatches is { Count: > 0 } m
            ? string.Join("; ", m.Select(c => $"{c.Column}: '{c.SourceValue}' vs '{c.DestinationValue}'"))
            : "";
        var vm = new RowDiffSelectableVm(d.Kind, d.KeyDisplay, detail);
        if (d.ColumnMismatches is { Count: > 0 } cols)
        {
            foreach (var c in cols)
                vm.ColumnDiffs.Add(new ColumnDiffRowVm(c.Column, c.SourceValue, c.DestinationValue));
        }

        return vm;
    }
}
