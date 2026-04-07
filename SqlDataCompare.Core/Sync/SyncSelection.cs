using SqlDataCompare.Compare;

namespace SqlDataCompare.Sync;

/// <summary>Restricts sync to chosen tables and/or specific differing rows (matched by compare key + difference kind).</summary>
public sealed class SyncSelection
{
    /// <summary>When non-null, only these source table display names are synced. When null, every table in the sync worklist is eligible.</summary>
    public HashSet<string>? IncludedSourceTables { get; init; }

    /// <summary>
    /// Optional per-row inclusion. Key = source table display (same as <see cref="TablePairCompareResult.SourceTable"/>).
    /// Value = keys from <see cref="FormatRowKey"/>.
    /// If the dictionary has no entry for a table, all rows on that table are eligible (subject to Insert/Update/Delete flags).
    /// If the entry is an empty set, no rows are synced for that table.
    /// </summary>
    public Dictionary<string, HashSet<string>>? RowsBySourceTable { get; init; }

    public static string FormatRowKey(RowDifferenceKind kind, string keyDisplay) =>
        $"{(int)kind}\u001F{keyDisplay}";
}
