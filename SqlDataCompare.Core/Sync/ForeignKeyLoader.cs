using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Sync;

/// <summary>
/// Loads foreign key references from a destination database so that
/// <see cref="TableSyncOrderer"/> can topologically sort tables before sync.
/// </summary>
internal static class ForeignKeyLoader
{
    /// <summary>
    /// Returns a list of (referencingTable, referencedTable) pairs — i.e.
    /// "referencingTable has a FK that points at referencedTable".
    /// Only pairs where both sides appear in <paramref name="tables"/> are returned.
    /// </summary>
    public static async Task<IReadOnlyList<(TableRef Referencing, TableRef Referenced)>> LoadAsync(
        DatabaseEndpoint endpoint,
        CompareOptions options,
        IReadOnlyCollection<TableRef> tables,
        CancellationToken cancellationToken = default)
    {
        var cs = Connection.DatabaseConnectionResolver.ResolveEffectiveConnectionString(endpoint);
        var kind = DatabaseProviderNames.Parse(endpoint.Provider);
        var timeout = options.CommandTimeoutSeconds > 0 ? options.CommandTimeoutSeconds : 120;

        var tableSet = tables.ToHashSet(TableRefEqualityComparer.OrdinalIgnoreCase);

        return kind switch
        {
            DatabaseProviderKind.SqlServer  => await LoadSqlServerAsync(cs, timeout, tableSet, cancellationToken),
            DatabaseProviderKind.PostgreSql => await LoadPostgresAsync(cs, timeout, tableSet, cancellationToken),
            DatabaseProviderKind.MySql      => await LoadMySqlAsync(cs, timeout, tableSet, cancellationToken),
            _ => [],
        };
    }

    private static async Task<IReadOnlyList<(TableRef, TableRef)>> LoadSqlServerAsync(
        string cs, int timeout, HashSet<TableRef> tables, CancellationToken ct)
    {
        const string sql = """
            SELECT
                fk_schema  = OBJECT_SCHEMA_NAME(fk.parent_object_id),
                fk_table   = OBJECT_NAME(fk.parent_object_id),
                ref_schema = OBJECT_SCHEMA_NAME(fk.referenced_object_id),
                ref_table  = OBJECT_NAME(fk.referenced_object_id)
            FROM sys.foreign_keys fk
            WHERE fk.is_disabled = 0
            """;

        await using var conn = new SqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = timeout };
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadPairsAsync(reader, tables, ct);
    }

    private static async Task<IReadOnlyList<(TableRef, TableRef)>> LoadPostgresAsync(
        string cs, int timeout, HashSet<TableRef> tables, CancellationToken ct)
    {
        const string sql = """
            SELECT
                fk_schema  = tc.table_schema,
                fk_table   = tc.table_name,
                ref_schema = ccu.table_schema,
                ref_table  = ccu.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.referential_constraints rc
              ON rc.constraint_schema = tc.constraint_schema
             AND rc.constraint_name   = tc.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_schema = rc.unique_constraint_schema
             AND ccu.constraint_name   = rc.unique_constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            GROUP BY tc.table_schema, tc.table_name, ccu.table_schema, ccu.table_name
            """;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = timeout };
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadPairsAsync(reader, tables, ct);
    }

    private static async Task<IReadOnlyList<(TableRef, TableRef)>> LoadMySqlAsync(
        string cs, int timeout, HashSet<TableRef> tables, CancellationToken ct)
    {
        // MySQL stores FK info in INFORMATION_SCHEMA; TABLE_SCHEMA is the DB name.
        const string sql = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                REFERENCED_TABLE_SCHEMA,
                REFERENCED_TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE REFERENCED_TABLE_NAME IS NOT NULL
            GROUP BY TABLE_SCHEMA, TABLE_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
            """;

        await using var conn = new MySqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn) { CommandTimeout = timeout };
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadPairsAsync(reader, tables, ct);
    }

    private static async Task<List<(TableRef, TableRef)>> ReadPairsAsync(
        System.Data.Common.DbDataReader reader,
        HashSet<TableRef> tables,
        CancellationToken ct)
    {
        var pairs = new List<(TableRef, TableRef)>();
        while (await reader.ReadAsync(ct))
        {
            var fkRef  = new TableRef(reader.GetString(0), reader.GetString(1));
            var refRef = new TableRef(reader.GetString(2), reader.GetString(3));

            // Only include edges where both sides are in our worklist
            if (tables.Contains(fkRef) && tables.Contains(refRef) && fkRef != refRef)
                pairs.Add((fkRef, refRef));
        }
        return pairs;
    }
}

/// <summary>TableRef equality comparer using OrdinalIgnoreCase for schema and name.</summary>
internal sealed class TableRefEqualityComparer : IEqualityComparer<TableRef>
{
    public static readonly TableRefEqualityComparer OrdinalIgnoreCase = new();

    public bool Equals(TableRef x, TableRef y) =>
        StringComparer.OrdinalIgnoreCase.Equals(x.Schema, y.Schema) &&
        StringComparer.OrdinalIgnoreCase.Equals(x.Name, y.Name);

    public int GetHashCode(TableRef t) =>
        HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(t.Schema),
            StringComparer.OrdinalIgnoreCase.GetHashCode(t.Name));
}
