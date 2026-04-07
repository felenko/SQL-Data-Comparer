using SqlDataCompare.Schema;

namespace SqlDataCompare.DataSources;

public interface IDatabaseGateway : IAsyncDisposable
{
    Task<IReadOnlyList<TableRef>> ListTablesAsync(string? schemaIncludePattern, CancellationToken cancellationToken);
    Task<TableSchema?> GetTableSchemaAsync(TableRef table, CancellationToken cancellationToken);
    Task<IReadOnlyList<Dictionary<string, object?>>> ReadRowsOrderedAsync(
        TableRef table,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken);

    /// <summary>
    /// Executes multiple SQL statements sequentially on a single open connection.
    /// Each statement is paired with its positional parameter values.
    /// Returns the total number of rows affected.
    /// </summary>
    Task<long> ExecuteBatchNonQueryAsync(
        IReadOnlyList<(string Sql, IReadOnlyList<object?> Parameters)> statements,
        CancellationToken cancellationToken = default,
        IProgress<(int completedStatements, int totalStatements)>? batchProgress = null);
}
