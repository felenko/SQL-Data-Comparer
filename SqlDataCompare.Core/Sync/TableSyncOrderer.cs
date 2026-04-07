using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Sync;

/// <summary>
/// Orders a list of tables for safe sync using a topological sort (Kahn's algorithm).
/// Tables with no FK dependencies come first; tables that are referenced by others come last.
/// </summary>
internal static class TableSyncOrderer
{
    /// <summary>
    /// Sorts <paramref name="tables"/> so that for every FK edge
    /// (referencing → referenced), the referenced table appears before the referencing one
    /// for INSERT/UPDATE operations.
    ///
    /// Tables in cycles are placed at the end with a warning; this is expected when
    /// <see cref="SyncOptions.DisableForeignKeyChecks"/> is true.
    /// </summary>
    /// <returns>
    /// Sorted list (safe INSERT order) and any tables that could not be ordered due to
    /// circular FK references.
    /// </returns>
    public static (IReadOnlyList<TableRef> Sorted, IReadOnlyList<TableRef> Cycles)
        Sort(
            IReadOnlyList<TableRef> tables,
            IReadOnlyList<(TableRef Referencing, TableRef Referenced)> foreignKeys)
    {
        // Build adjacency: for INSERT order, a referencing table depends on its referenced table.
        // So "referenced" must come before "referencing".
        // In Kahn's algorithm: edge  referenced → referencing  (referenced is a prerequisite).

        var cmp = TableRefEqualityComparer.OrdinalIgnoreCase;
        var tableSet = new HashSet<TableRef>(tables, cmp);

        // in-degree for each table
        var inDegree = new Dictionary<TableRef, int>(cmp);
        // adjacency list: for each "referenced" table, who comes after it?
        var successors = new Dictionary<TableRef, List<TableRef>>(cmp);

        foreach (var t in tables)
        {
            inDegree[t] = 0;
            successors[t] = new List<TableRef>();
        }

        foreach (var (referencing, referenced) in foreignKeys)
        {
            if (!tableSet.Contains(referencing) || !tableSet.Contains(referenced))
                continue;

            // referenced must appear before referencing
            successors[referenced].Add(referencing);
            inDegree[referencing]++;
        }

        // Kahn's algorithm
        var queue = new Queue<TableRef>(
            tables.Where(t => inDegree[t] == 0));

        var sorted = new List<TableRef>(tables.Count);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            sorted.Add(current);

            foreach (var next in successors[current])
            {
                inDegree[next]--;
                if (inDegree[next] == 0)
                    queue.Enqueue(next);
            }
        }

        // Any tables still with in-degree > 0 are in a cycle
        var cycles = tables
            .Where(t => inDegree[t] > 0)
            .ToList();

        // Append cycle members at the end so they still get processed
        sorted.AddRange(cycles);

        return (sorted, cycles);
    }

    /// <summary>
    /// Reverses the insert order to get safe DELETE order
    /// (child tables deleted before parent tables).
    /// </summary>
    public static IReadOnlyList<TableRef> ReverseForDelete(IReadOnlyList<TableRef> insertOrder) =>
        insertOrder.Reverse().ToList();
}
