using SqlDataCompare.DataSources;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Sync;

internal static class SyncSqlBuilder
{
    /// <summary>
    /// Builds a parameterized INSERT statement.
    /// <paramref name="values"/> is a list of (destination column name, value) pairs.
    /// </summary>
    public static (string Sql, IReadOnlyList<object?> Parameters) BuildInsert(
        TableRef table,
        IReadOnlyList<(string DestCol, object? Value)> values,
        string provider)
    {
        var (qtable, qcol, pref) = Dialect(provider);
        var cols = string.Join(", ", values.Select(v => qcol(v.DestCol)));
        var parms = string.Join(", ", values.Select((_, i) => pref(i)));
        var sql = $"INSERT INTO {qtable(table)} ({cols}) VALUES ({parms})";
        return (sql, values.Select(v => v.Value).ToList());
    }

    /// <summary>
    /// Builds a parameterized UPDATE statement.
    /// <paramref name="setValues"/> is (dest col, new value) for non-key columns.
    /// <paramref name="whereValues"/> is (dest col, key value) for WHERE clause.
    /// </summary>
    public static (string Sql, IReadOnlyList<object?> Parameters) BuildUpdate(
        TableRef table,
        IReadOnlyList<(string DestCol, object? Value)> setValues,
        IReadOnlyList<(string DestCol, object? Value)> whereValues,
        string provider)
    {
        var (qtable, qcol, pref) = Dialect(provider);
        var idx = 0;
        var setParts = setValues.Select(v => $"{qcol(v.DestCol)} = {pref(idx++)}").ToList();
        var whereParts = whereValues.Select(v => $"{qcol(v.DestCol)} = {pref(idx++)}").ToList();
        var sql = $"UPDATE {qtable(table)} SET {string.Join(", ", setParts)} WHERE {string.Join(" AND ", whereParts)}";
        var allParams = setValues.Concat(whereValues).Select(v => v.Value).ToList();
        return (sql, allParams);
    }

    /// <summary>
    /// Builds a parameterized DELETE statement.
    /// <paramref name="whereValues"/> is (dest col, key value) for WHERE clause.
    /// </summary>
    public static (string Sql, IReadOnlyList<object?> Parameters) BuildDelete(
        TableRef table,
        IReadOnlyList<(string DestCol, object? Value)> whereValues,
        string provider)
    {
        var (qtable, qcol, pref) = Dialect(provider);
        var whereParts = whereValues.Select((v, i) => $"{qcol(v.DestCol)} = {pref(i)}").ToList();
        var sql = $"DELETE FROM {qtable(table)} WHERE {string.Join(" AND ", whereParts)}";
        return (sql, whereValues.Select(v => v.Value).ToList());
    }

    public static string QuoteTable(TableRef table, string provider)
    {
        var (qtable, _, _) = Dialect(provider);
        return qtable(table);
    }

    private static (Func<TableRef, string> QTable, Func<string, string> QCol, Func<int, string> PRef)
        Dialect(string provider)
    {
        return DatabaseProviderNames.Parse(provider) switch
        {
            DatabaseProviderKind.SqlServer => (
                SqlIdentifier.SqlServerQuoteTable,
                SqlIdentifier.SqlServerQuoteColumn,
                i => $"@p{i}"
            ),
            DatabaseProviderKind.PostgreSql => (
                SqlIdentifier.PostgresQuoteTable,
                SqlIdentifier.PostgresQuoteColumn,
                i => $"${i + 1}"
            ),
            DatabaseProviderKind.MySql => (
                SqlIdentifier.MySqlQuoteTable,
                SqlIdentifier.MySqlQuoteColumn,
                _ => "?"
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(provider)),
        };
    }
}
