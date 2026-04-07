namespace SqlDataCompare.Compare;

/// <summary>Incremental progress while <see cref="DataCompareService.RunAsync"/> runs.</summary>
public sealed class CompareProgressInfo
{
    /// <summary>Number of table results produced so far (0 before any table completes).</summary>
    public int CompletedTables { get; init; }

    /// <summary>Total table results expected (including worklist validation errors).</summary>
    public int TotalTables { get; init; }

    /// <summary>The table result just completed, or null when reporting totals only.</summary>
    public TablePairCompareResult? LatestTable { get; init; }
}
