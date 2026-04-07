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

    /// <summary>Source table display name for the in-flight compare (merge or load), when applicable.</summary>
    public string? ActiveSourceTable { get; init; }

    /// <summary>
    /// During row merge for the current table: combined scan position (source index + dest index), up to
    /// <see cref="RowsTotal"/> which is source row count + destination row count.
    /// Null when not in merge or after the table finishes.
    /// </summary>
    public int? RowsCompared { get; init; }

    /// <summary>Source row count + destination row count for the table currently merging; null if not applicable.</summary>
    public int? RowsTotal { get; init; }
}
