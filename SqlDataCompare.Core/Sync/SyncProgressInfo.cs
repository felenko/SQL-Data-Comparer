namespace SqlDataCompare.Sync;

public enum SyncProgressPhase
{
    /// <summary>Sync run started; <see cref="SyncProgressInfo.TablesTotal"/> is set.</summary>
    Started,
    /// <summary>Beginning work on a table (before load/execute).</summary>
    TableStarting,
    /// <summary>Loading schemas and row data for the current table.</summary>
    LoadingRows,
    /// <summary>Executing SQL statements on the destination (see completed/total).</summary>
    ExecutingBatch,
    /// <summary>Finished current table (success, skip, or error handled in result).</summary>
    TableCompleted,
    /// <summary>All tables processed.</summary>
    Finished,
}

/// <summary>Progress updates during <see cref="DataSyncService.RunAsync"/>.</summary>
public sealed class SyncProgressInfo
{
    public SyncProgressPhase Phase { get; init; }

    /// <summary>Total tables in the sync worklist (same order as processing).</summary>
    public int TablesTotal { get; init; }

    /// <summary>1-based index of the table currently being processed.</summary>
    public int TableIndex { get; init; }

    public string? SourceTable { get; init; }
    public string? DestinationTable { get; init; }

    /// <summary>Statements completed in the current batch (meaningful in <see cref="SyncProgressPhase.ExecutingBatch"/>).</summary>
    public int CompletedStatements { get; init; }

    /// <summary>Total SQL statements in the current batch (includes identity SET if used).</summary>
    public int TotalStatements { get; init; }
}
