using System.Globalization;
using SqlDataCompare.DataSources;
using SqlDataCompare.Schema;
using SqlDataCompare.Sql;

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

    /// <summary>
    /// Returns (disableSql, enableSql) to wrap a table's sync batch with FK-check suppression.
    /// <para>
    /// SQL Server: per-table <c>ALTER TABLE … NOCHECK / WITH CHECK CHECK CONSTRAINT ALL</c>
    /// — persistent (not connection-scoped), so it survives across connection re-opens.
    /// </para>
    /// <para>
    /// PostgreSQL: <c>SET session_replication_role</c> — connection-scoped, must be in the
    /// same <c>ExecuteBatchNonQueryAsync</c> call.
    /// </para>
    /// <para>
    /// MySQL: <c>SET FOREIGN_KEY_CHECKS</c> — connection-scoped, same requirement.
    /// </para>
    /// </summary>
    public static (string Disable, string Enable) GetFkCheckWrapSql(TableRef destTable, string provider) =>
        DatabaseProviderNames.Parse(provider) switch
        {
            DatabaseProviderKind.SqlServer => (
                $"ALTER TABLE {SqlIdentifier.SqlServerQuoteTable(destTable)} NOCHECK CONSTRAINT ALL",
                $"ALTER TABLE {SqlIdentifier.SqlServerQuoteTable(destTable)} WITH CHECK CHECK CONSTRAINT ALL"
            ),
            DatabaseProviderKind.PostgreSql => (
                "SET session_replication_role = replica",
                "SET session_replication_role = DEFAULT"
            ),
            DatabaseProviderKind.MySql => (
                "SET FOREIGN_KEY_CHECKS = 0",
                "SET FOREIGN_KEY_CHECKS = 1"
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(provider)),
        };

    /// <summary>Quotes a table identifier using the INSERT folder's SQL dialect.</summary>
    public static string QuoteTableForDialect(TableRef table, InsertSqlDialect dialect) => dialect switch
    {
        InsertSqlDialect.SqlServer => SqlIdentifier.SqlServerQuoteTable(table),
        InsertSqlDialect.PostgreSql => SqlIdentifier.PostgresQuoteTable(table),
        InsertSqlDialect.MySql => SqlIdentifier.MySqlQuoteTable(table),
        _ => throw new ArgumentOutOfRangeException(nameof(dialect)),
    };

    /// <summary>Quotes a column identifier using the INSERT folder's SQL dialect.</summary>
    public static string QuoteColumnForDialect(string column, InsertSqlDialect dialect) => dialect switch
    {
        InsertSqlDialect.SqlServer => SqlIdentifier.SqlServerQuoteColumn(column),
        InsertSqlDialect.PostgreSql => SqlIdentifier.PostgresQuoteColumn(column),
        InsertSqlDialect.MySql => SqlIdentifier.MySqlQuoteColumn(column),
        _ => throw new ArgumentOutOfRangeException(nameof(dialect)),
    };

    /// <summary>
    /// Formats a CLR value as a SQL literal suitable for writing to an INSERT SQL file.
    /// </summary>
    public static string FormatLiteralValue(object? value, InsertSqlDialect dialect)
    {
        if (value is null) return "NULL";
        if (value is string s) return $"'{s.Replace("'", "''")}'";
        if (value is bool b) return dialect == InsertSqlDialect.PostgreSql ? (b ? "TRUE" : "FALSE") : (b ? "1" : "0");
        if (value is byte or sbyte or short or ushort or int or uint or long or ulong) return value.ToString()!;
        if (value is decimal d) return d.ToString(CultureInfo.InvariantCulture);
        if (value is float f) return f.ToString(CultureInfo.InvariantCulture);
        if (value is double dbl) return dbl.ToString(CultureInfo.InvariantCulture);
        if (value is DateTime dt) return $"'{dt:yyyy-MM-ddTHH:mm:ss.fff}'";
        if (value is DateTimeOffset dto) return $"'{dto:yyyy-MM-ddTHH:mm:ss.fffzzz}'";
        if (value is Guid g) return $"'{g}'";
        if (value is byte[] bytes)
        {
            var hex = Convert.ToHexString(bytes);
            return dialect == InsertSqlDialect.PostgreSql ? $"'\\x{hex}'" : $"0x{hex}";
        }
        var str = value.ToString() ?? "";
        return $"'{str.Replace("'", "''")}'";
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
