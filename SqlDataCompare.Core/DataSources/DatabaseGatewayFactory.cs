using System.Data;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.DataSources;

public static class DatabaseGatewayFactory
{
    public static IDatabaseGateway Create(DatabaseEndpoint endpoint, CompareOptions options)
    {
        var kind = DatabaseProviderNames.Parse(endpoint.Provider);
        var cs = Connection.DatabaseConnectionResolver.ResolveEffectiveConnectionString(endpoint);
        var timeout = options.CommandTimeoutSeconds > 0 ? options.CommandTimeoutSeconds : 120;
        return kind switch
        {
            DatabaseProviderKind.SqlServer => new SqlServerGateway(cs, timeout),
            DatabaseProviderKind.PostgreSql => new NpgsqlGateway(cs, timeout),
            DatabaseProviderKind.MySql => new MySqlGateway(cs, timeout),
            _ => throw new ArgumentOutOfRangeException(),
        };
    }
}

internal sealed class SqlServerGateway : IDatabaseGateway
{
    private readonly string _connectionString;
    private readonly int _timeout;

    public SqlServerGateway(string connectionString, int timeoutSeconds)
    {
        _connectionString = connectionString;
        _timeout = timeoutSeconds;
    }

    public async Task<IReadOnlyList<TableRef>> ListTablesAsync(string? schemaIncludePattern, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var filter = SchemaFilterSql.SqlServer(schemaIncludePattern);
        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = _timeout;
        cmd.CommandText = $"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'{filter}
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
        var list = new List<TableRef>();
        await using var r = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await r.ReadAsync(cancellationToken))
        {
            list.Add(new TableRef(r.GetString(0), r.GetString(1)));
        }

        return list;
    }

    public async Task<TableSchema?> GetTableSchemaAsync(TableRef table, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var pk = await ReadPkSqlServerAsync(conn, table, _timeout, cancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = _timeout;
        cmd.CommandText = """
            SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
                   COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA)+N'.'+QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, N'IsIdentity') AS IsIdentity
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
            ORDER BY c.ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@schema", table.Schema);
        cmd.Parameters.AddWithValue("@table", table.Name);
        var cols = new List<ColumnDefinition>();
        await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await r.ReadAsync(cancellationToken))
            {
                cols.Add(new ColumnDefinition
                {
                    Name = r.GetString(0),
                    PhysicalType = r.GetString(1),
                    IsNullable = string.Equals(r.GetString(2), "YES", StringComparison.OrdinalIgnoreCase),
                    IsIdentity = !r.IsDBNull(3) && Convert.ToInt32(r.GetValue(3)) != 0,
                });
            }
        }

        if (cols.Count == 0)
            return null;
        return new TableSchema
        {
            Table = table,
            Columns = cols,
            PrimaryKeyColumns = pk,
        };
    }

    public async Task<IReadOnlyList<Dictionary<string, object?>>> ReadRowsOrderedAsync(
        TableRef table,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var selectList = string.Join(", ", projectedColumns.Select(SqlIdentifier.SqlServerQuoteColumn));
        var orderList = string.Join(", ", keyColumns.Select(SqlIdentifier.SqlServerQuoteColumn));
        var fq = SqlIdentifier.SqlServerQuoteTable(table);
        var top = maxRows is > 0 ? $"TOP ({maxRows.Value}) " : "";
        var wherePart = string.IsNullOrWhiteSpace(whereClause) ? "" : $" AND ({whereClause})";
        var sql = $"SELECT {top}{selectList} FROM {fq} WHERE 1 = 1{wherePart} ORDER BY {orderList}";

        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = _timeout };
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await ReadRowsIntoListAsync(reader, projectedColumns, cancellationToken);
    }

    private static async Task<IReadOnlyList<string>> ReadPkSqlServerAsync(SqlConnection conn, TableRef table, int timeoutSeconds, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = timeoutSeconds;
        cmd.CommandText = """
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA AND tc.TABLE_NAME = ku.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = @schema AND tc.TABLE_NAME = @table AND tc.CONSTRAINT_TYPE = N'PRIMARY KEY'
            ORDER BY ku.ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@schema", table.Schema);
        cmd.Parameters.AddWithValue("@table", table.Name);
        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            list.Add(r.GetString(0));
        return list;
    }

    private static async Task<List<Dictionary<string, object?>>> ReadRowsIntoListAsync(
        SqlDataReader reader,
        IReadOnlyList<string> projectedColumns,
        CancellationToken cancellationToken)
    {
        var rows = new List<Dictionary<string, object?>>();
        var ordinals = projectedColumns.Select(c => reader.GetOrdinal(c)).ToArray();
        while (await reader.ReadAsync(cancellationToken))
        {
            var d = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var i = 0; i < projectedColumns.Count; i++)
            {
                var o = ordinals[i];
                d[projectedColumns[i]] = reader.IsDBNull(o) ? null : reader.GetValue(o);
            }

            rows.Add(d);
        }

        return rows;
    }

    public async Task<long> ExecuteBatchNonQueryAsync(
        IReadOnlyList<(string Sql, IReadOnlyList<object?> Parameters)> statements,
        CancellationToken cancellationToken = default,
        IProgress<(int completedStatements, int totalStatements)>? batchProgress = null)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        long total = 0;
        var n = statements.Count;
        for (var idx = 0; idx < n; idx++)
        {
            var (sql, parameters) = statements[idx];
            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = _timeout };
            for (var i = 0; i < parameters.Count; i++)
                cmd.Parameters.Add(CreateSqlServerParameter($"@p{i}", parameters[i]));
            total += await cmd.ExecuteNonQueryAsync(cancellationToken);
            batchProgress?.Report((idx + 1, n));
        }
        return total;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>
    /// <see cref="SqlCommand.Parameters.AddWithValue"/> infers <c>nvarchar</c> for some CLR types; binding those to
    /// <c>image</c>/<c>varbinary</c> columns causes SQL Server error 402 (operator type clash).
    /// </summary>
    private static SqlParameter CreateSqlServerParameter(string name, object? value)
    {
        if (value is null || value is DBNull)
            return new SqlParameter(name, DBNull.Value);
        if (value is byte[] bytes)
            return new SqlParameter(name, SqlDbType.VarBinary, -1) { Value = bytes };
        return new SqlParameter(name, value);
    }
}

internal sealed class NpgsqlGateway : IDatabaseGateway
{
    private readonly string _connectionString;
    private readonly int _timeout;

    public NpgsqlGateway(string connectionString, int timeoutSeconds)
    {
        _connectionString = connectionString;
        _timeout = timeoutSeconds;
    }

    public async Task<IReadOnlyList<TableRef>> ListTablesAsync(string? schemaIncludePattern, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var filter = SchemaFilterSql.PostgreSql(schemaIncludePattern);
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT table_schema, table_name
             FROM information_schema.tables
             WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema'){filter}
             ORDER BY table_schema, table_name
             """, conn) { CommandTimeout = _timeout };
        var list = new List<TableRef>();
        await using var r = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await r.ReadAsync(cancellationToken))
            list.Add(new TableRef(r.GetString(0), r.GetString(1)));
        return list;
    }

    public async Task<TableSchema?> GetTableSchemaAsync(TableRef table, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var pk = await ReadPkPostgresAsync(conn, table, cancellationToken);

        await using var cmd = new NpgsqlCommand(
            """
            SELECT column_name, data_type, is_nullable,
                   ((is_identity = 'YES') OR (column_default IS NOT NULL AND column_default ILIKE 'nextval%')) AS is_identityish
            FROM information_schema.columns
            WHERE table_schema = @s AND table_name = @t
            ORDER BY ordinal_position
            """, conn) { CommandTimeout = _timeout };
        cmd.Parameters.AddWithValue("s", table.Schema);
        cmd.Parameters.AddWithValue("t", table.Name);
        var cols = new List<ColumnDefinition>();
        await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await r.ReadAsync(cancellationToken))
            {
                cols.Add(new ColumnDefinition
                {
                    Name = r.GetString(0),
                    PhysicalType = r.GetString(1),
                    IsNullable = string.Equals(r.GetString(2), "YES", StringComparison.OrdinalIgnoreCase),
                    IsIdentity = r.GetBoolean(3),
                });
            }
        }

        if (cols.Count == 0)
            return null;
        return new TableSchema { Table = table, Columns = cols, PrimaryKeyColumns = pk };
    }

    public async Task<IReadOnlyList<Dictionary<string, object?>>> ReadRowsOrderedAsync(
        TableRef table,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var selectList = string.Join(", ", projectedColumns.Select(SqlIdentifier.PostgresQuoteColumn));
        var orderList = string.Join(", ", keyColumns.Select(SqlIdentifier.PostgresQuoteColumn));
        var fq = SqlIdentifier.PostgresQuoteTable(table);
        var wherePart = string.IsNullOrWhiteSpace(whereClause) ? "" : $" AND ({whereClause})";
        var limit = maxRows is > 0 ? $" LIMIT {maxRows.Value}" : "";
        var sql = $"SELECT {selectList} FROM {fq} WHERE TRUE{wherePart} ORDER BY {orderList}{limit}";
        await using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = _timeout };
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await ReadRowsIntoListAsync(reader, projectedColumns, cancellationToken);
    }

    private static async Task<IReadOnlyList<string>> ReadPkPostgresAsync(NpgsqlConnection conn, TableRef table, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = @s AND tc.table_name = @t AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """, conn) { CommandTimeout = conn.ConnectionTimeout };
        cmd.Parameters.AddWithValue("s", table.Schema);
        cmd.Parameters.AddWithValue("t", table.Name);
        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            list.Add(r.GetString(0));
        return list;
    }

    private static async Task<List<Dictionary<string, object?>>> ReadRowsIntoListAsync(
        NpgsqlDataReader reader,
        IReadOnlyList<string> projectedColumns,
        CancellationToken cancellationToken)
    {
        var rows = new List<Dictionary<string, object?>>();
        var ordinals = projectedColumns.Select(c => reader.GetOrdinal(c)).ToArray();
        while (await reader.ReadAsync(cancellationToken))
        {
            var d = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var i = 0; i < projectedColumns.Count; i++)
            {
                var o = ordinals[i];
                d[projectedColumns[i]] = reader.IsDBNull(o) ? null : reader.GetValue(o);
            }

            rows.Add(d);
        }

        return rows;
    }

    public async Task<long> ExecuteBatchNonQueryAsync(
        IReadOnlyList<(string Sql, IReadOnlyList<object?> Parameters)> statements,
        CancellationToken cancellationToken = default,
        IProgress<(int completedStatements, int totalStatements)>? batchProgress = null)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        long total = 0;
        var n = statements.Count;
        for (var idx = 0; idx < n; idx++)
        {
            var (sql, parameters) = statements[idx];
            await using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = _timeout };
            foreach (var p in parameters)
                cmd.Parameters.Add(new Npgsql.NpgsqlParameter { Value = p ?? DBNull.Value });
            total += await cmd.ExecuteNonQueryAsync(cancellationToken);
            batchProgress?.Report((idx + 1, n));
        }
        return total;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class MySqlGateway : IDatabaseGateway
{
    private readonly string _connectionString;
    private readonly int _timeout;

    public MySqlGateway(string connectionString, int timeoutSeconds)
    {
        _connectionString = connectionString;
        _timeout = timeoutSeconds;
    }

    public async Task<IReadOnlyList<TableRef>> ListTablesAsync(string? schemaIncludePattern, CancellationToken cancellationToken)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var db = conn.Database;
        if (string.IsNullOrEmpty(db))
            throw new InvalidOperationException("MySQL connection string must specify Database=...");
        _ = schemaIncludePattern;
        await using var cmd = new MySqlCommand(
            """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @db
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """, conn) { CommandTimeout = _timeout };
        cmd.Parameters.AddWithValue("@db", db);
        var list = new List<TableRef>();
        await using var r = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await r.ReadAsync(cancellationToken))
            list.Add(new TableRef(r.GetString(0), r.GetString(1)));
        return list;
    }

    public async Task<TableSchema?> GetTableSchemaAsync(TableRef table, CancellationToken cancellationToken)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var pk = await ReadPkMySqlAsync(conn, table, cancellationToken);
        await using var cmd = new MySqlCommand(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @s AND TABLE_NAME = @t
            ORDER BY ORDINAL_POSITION
            """, conn) { CommandTimeout = _timeout };
        cmd.Parameters.AddWithValue("@s", table.Schema);
        cmd.Parameters.AddWithValue("@t", table.Name);
        var cols = new List<ColumnDefinition>();
        await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await r.ReadAsync(cancellationToken))
            {
                var extra = r.FieldCount > 3 && !r.IsDBNull(3) ? r.GetString(3) : "";
                cols.Add(new ColumnDefinition
                {
                    Name = r.GetString(0),
                    PhysicalType = r.GetString(1),
                    IsNullable = string.Equals(r.GetString(2), "YES", StringComparison.OrdinalIgnoreCase),
                    IsIdentity = extra.Contains("auto_increment", StringComparison.OrdinalIgnoreCase),
                });
            }
        }

        if (cols.Count == 0)
            return null;
        return new TableSchema { Table = table, Columns = cols, PrimaryKeyColumns = pk };
    }

    public async Task<IReadOnlyList<Dictionary<string, object?>>> ReadRowsOrderedAsync(
        TableRef table,
        IReadOnlyList<string> projectedColumns,
        IReadOnlyList<string> keyColumns,
        string? whereClause,
        int? maxRows,
        CancellationToken cancellationToken)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        var selectList = string.Join(", ", projectedColumns.Select(SqlIdentifier.MySqlQuoteColumn));
        var orderList = string.Join(", ", keyColumns.Select(SqlIdentifier.MySqlQuoteColumn));
        var fq = SqlIdentifier.MySqlQuoteTable(table);
        var wherePart = string.IsNullOrWhiteSpace(whereClause) ? "" : $" AND ({whereClause})";
        var limit = maxRows is > 0 ? $" LIMIT {maxRows.Value}" : "";
        var sql = $"SELECT {selectList} FROM {fq} WHERE TRUE{wherePart} ORDER BY {orderList}{limit}";
        await using var cmd = new MySqlCommand(sql, conn) { CommandTimeout = _timeout };
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await ReadRowsIntoListAsync(reader, projectedColumns, cancellationToken);
    }

    private static async Task<IReadOnlyList<string>> ReadPkMySqlAsync(MySqlConnection conn, TableRef table, CancellationToken ct)
    {
        await using var cmd = new MySqlCommand(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
              ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
             AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
             AND kcu.TABLE_NAME = tc.TABLE_NAME
            WHERE kcu.TABLE_SCHEMA = @s AND kcu.TABLE_NAME = @t AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """, conn);
        cmd.Parameters.AddWithValue("@s", table.Schema);
        cmd.Parameters.AddWithValue("@t", table.Name);
        var list = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
            list.Add(r.GetString(0));
        return list;
    }

    private static async Task<List<Dictionary<string, object?>>> ReadRowsIntoListAsync(
        MySqlDataReader reader,
        IReadOnlyList<string> projectedColumns,
        CancellationToken cancellationToken)
    {
        var rows = new List<Dictionary<string, object?>>();
        var ordinals = projectedColumns.Select(c => reader.GetOrdinal(c)).ToArray();
        while (await reader.ReadAsync(cancellationToken))
        {
            var d = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var i = 0; i < projectedColumns.Count; i++)
            {
                var o = ordinals[i];
                d[projectedColumns[i]] = reader.IsDBNull(o) ? null : reader.GetValue(o);
            }

            rows.Add(d);
        }

        return rows;
    }

    public async Task<long> ExecuteBatchNonQueryAsync(
        IReadOnlyList<(string Sql, IReadOnlyList<object?> Parameters)> statements,
        CancellationToken cancellationToken = default,
        IProgress<(int completedStatements, int totalStatements)>? batchProgress = null)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        long total = 0;
        var n = statements.Count;
        for (var idx = 0; idx < n; idx++)
        {
            var (sql, parameters) = statements[idx];
            await using var cmd = new MySqlCommand(sql, conn) { CommandTimeout = _timeout };
            foreach (var p in parameters)
                cmd.Parameters.Add(new MySqlParameter { Value = p ?? DBNull.Value });
            total += await cmd.ExecuteNonQueryAsync(cancellationToken);
            batchProgress?.Report((idx + 1, n));
        }
        return total;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal static class SchemaFilterSql
{
    public static string SqlServer(string? schemaPattern)
    {
        if (string.IsNullOrWhiteSpace(schemaPattern) || schemaPattern == "*")
            return "";
        return $" AND TABLE_SCHEMA = N'{schemaPattern.Replace("'", "''")}'";
    }

    public static string PostgreSql(string? schemaPattern)
    {
        if (string.IsNullOrWhiteSpace(schemaPattern) || schemaPattern == "*")
            return "";
        return $" AND table_schema = '{schemaPattern.Replace("'", "''")}'";
    }

}

internal static class SqlIdentifier
{
    public static string SqlServerQuoteColumn(string name) => $"[{name.Replace("]", "]]")}]";

    public static string SqlServerQuoteTable(TableRef t) =>
        $"[{t.Schema.Replace("]", "]]")}].[{t.Name.Replace("]", "]]")}]";

    public static string PostgresQuoteColumn(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    public static string PostgresQuoteTable(TableRef t) =>
        $"\"{t.Schema.Replace("\"", "\"\"")}\".\"{t.Name.Replace("\"", "\"\"")}\"";

    public static string MySqlQuoteColumn(string name) => $"`{name.Replace("`", "``")}`";

    public static string MySqlQuoteTable(TableRef t) =>
        $"`{t.Schema.Replace("`", "``")}`.`{t.Name.Replace("`", "``")}`";
}
