namespace SqlDataCompare.Compare;

public enum TableCompareStatus
{
    Identical,
    Different,
    SampledDifferent,
    Error,
    Skipped,
}

public sealed class ProjectCompareResult
{
    public required string ProjectName { get; init; }
    public required IReadOnlyList<TablePairCompareResult> Tables { get; init; }

    /// <summary>True when compare was stopped cooperatively; <see cref="Tables"/> contains only work completed before the stop.</summary>
    public bool Cancelled { get; init; }

    public bool AllIdentical => Tables.All(t => t.Status is TableCompareStatus.Identical or TableCompareStatus.Skipped);
}

public sealed class TablePairCompareResult
{
    public required string SourceTable { get; init; }
    public required string DestinationTable { get; init; }
    public TableCompareStatus Status { get; init; }
    public string? ErrorMessage { get; init; }
    public long RowsOnlyInSource { get; init; }
    public long RowsOnlyInDestination { get; init; }
    public long RowsWithValueDifferences { get; init; }
    public bool Sampled { get; init; }
    public IReadOnlyList<RowDifference> SampleDiffs { get; init; } = Array.Empty<RowDifference>();
    public KeyResolutionSummary? Keys { get; init; }
}

public sealed class KeyResolutionSummary
{
    public required string Confidence { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public string? Detail { get; init; }
}

public sealed class RowDifference
{
    public required string KeyDisplay { get; init; }
    public RowDifferenceKind Kind { get; init; }
    public IReadOnlyList<ColumnMismatch>? ColumnMismatches { get; init; }
}

public enum RowDifferenceKind
{
    MissingInDestination,
    MissingInSource,
    ValueMismatch,
}

public sealed class ColumnMismatch
{
    public required string Column { get; init; }
    public required string? SourceValue { get; init; }
    public required string? DestinationValue { get; init; }
}
