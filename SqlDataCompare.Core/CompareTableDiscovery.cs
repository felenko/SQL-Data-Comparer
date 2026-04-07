using SqlDataCompare.Compare;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare;

/// <summary>Public helper to list tables from a configured endpoint (used by the WPF workbench).</summary>
public static class CompareTableDiscovery
{
    public static async Task<IReadOnlyList<TableRef>> ListTablesAsync(
        DataEndpoint endpoint,
        CompareOptions options,
        CancellationToken cancellationToken = default)
    {
        await using var side = CompareSide.Create(endpoint, options, isSource: true);
        return await side.ListTablesAsync(cancellationToken);
    }
}
