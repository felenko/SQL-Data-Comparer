namespace SqlDataCompare.Sync;

public sealed class ProjectSyncResult
{
    public required IReadOnlyList<TableSyncResult> Tables { get; init; }
}

public sealed class TableSyncResult
{
    public required string SourceTable { get; init; }
    public required string DestinationTable { get; init; }
    public SyncStatus Status { get; init; }
    public long Inserted { get; init; }
    public long Updated { get; init; }
    public long Deleted { get; init; }
    public string? ErrorMessage { get; init; }
}

public enum SyncStatus { Success, Error, Skipped }

public sealed class SyncOptions
{
    public bool InsertMissing { get; set; } = true;
    public bool UpdateChanged { get; set; } = true;
    public bool DeleteExtra { get; set; } = false;

    /// <summary>Optional table/row filters from the UI. Null means sync everything the flags allow.</summary>
    public SyncSelection? Selection { get; set; }
}
