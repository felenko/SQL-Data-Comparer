using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using SqlDataCompare.Connection;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Mcp;

/// <summary>Foreign-key edges that touch a single table (for MCP / tooling).</summary>
public sealed class TableForeignKeyEdge
{
    public required string ReferencingSchema { get; init; }
    public required string ReferencingTable { get; init; }
    public required string ReferencedSchema { get; init; }
    public required string ReferencedTable { get; init; }

    public string ReferencingDisplay =>
        string.IsNullOrEmpty(ReferencingSchema) ? ReferencingTable : $"{ReferencingSchema}.{ReferencingTable}";

    public string ReferencedDisplay =>
        string.IsNullOrEmpty(ReferencedSchema) ? ReferencedTable : $"{ReferencedSchema}.{ReferencedTable}";
}

/// <summary>Lists foreign keys involving a table (incoming and outgoing).</summary>
public static class RelatedTableDiscovery
{
    public static async Task<(IReadOnlyList<TableForeignKeyEdge> Outgoing, IReadOnlyList<TableForeignKeyEdge> Incoming)>
        GetEdgesInvolvingTableAsync(
            DatabaseEndpoint endpoint,
            CompareOptions options,
            string tableSchema,
            string tableName,
            CancellationToken cancellationToken = default)
    {
        var cs = DatabaseConnectionResolver.ResolveEffectiveConnectionString(endpoint);
        var kind = DatabaseProviderNames.Parse(endpoint.Provider);
        var timeout = options.CommandTimeoutSeconds > 0 ? options.CommandTimeoutSeconds : 120;

        return kind switch
        {
            DatabaseProviderKind.SqlServer => await SqlServerAsync(cs, timeout, tableSchema, tableName, cancellationToken),
            DatabaseProviderKind.PostgreSql => await PostgresAsync(cs, timeout, tableSchema, tableName, cancellationToken),
            DatabaseProviderKind.MySql => await MySqlAsync(cs, timeout, tableSchema, tableName, cancellationToken),
            _ => ([], []),
        };
    }

    private static async Task<(List<TableForeignKeyEdge> Outgoing, List<TableForeignKeyEdge> Incoming)> SqlServerAsync(
        string cs, int timeout, string schema, string table, CancellationToken ct)
    {
        const string sql = """
            SELECT
                OBJECT_SCHEMA_NAME(fk.parent_object_id),
                OBJECT_NAME(fk.parent_object_id),
                OBJECT_SCHEMA_NAME(fk.referenced_object_id),
                OBJECT_NAME(fk.referenced_object_id)
            FROM sys.foreign_keys fk
            WHERE fk.is_disabled = 0
              AND (
                (OBJECT_SCHEMA_NAME(fk.parent_object_id) = @schema AND OBJECT_NAME(fk.parent_object_id) = @table)
                OR
                (OBJECT_SCHEMA_NAME(fk.referenced_object_id) = @schema AND OBJECT_NAME(fk.referenced_object_id) = @table)
              )
            """;

        await using var conn = new SqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = timeout };
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await ReadAndSplitAsync(r, schema, table, ct);
    }

    private static async Task<(List<TableForeignKeyEdge> Outgoing, List<TableForeignKeyEdge> Incoming)> PostgresAsync(
        string cs, int timeout, string schema, string table, CancellationToken ct)
    {
        const string sql = """
            SELECT
                tc.table_schema,
                tc.table_name,
                ccu.table_schema,
                ccu.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.referential_constraints rc
              ON rc.constraint_schema = tc.constraint_schema
             AND rc.constraint_name   = tc.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_schema = rc.unique_constraint_schema
             AND ccu.constraint_name   = rc.unique_constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND (
                (tc.table_schema = @schema AND tc.table_name = @table)
                OR
                (ccu.table_schema = @schema AND ccu.table_name = @table)
              )
            GROUP BY tc.table_schema, tc.table_name, ccu.table_schema, ccu.table_name
            """;

        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn) { CommandTimeout = timeout };
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadAndSplitAsync(reader, schema, table, ct);
    }

    private static async Task<(List<TableForeignKeyEdge> Outgoing, List<TableForeignKeyEdge> Incoming)> MySqlAsync(
        string cs, int timeout, string schema, string table, CancellationToken ct)
    {
        const string sql = """
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                REFERENCED_TABLE_SCHEMA,
                REFERENCED_TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE REFERENCED_TABLE_NAME IS NOT NULL
              AND (
                (TABLE_SCHEMA = @schema AND TABLE_NAME = @table)
                OR
                (REFERENCED_TABLE_SCHEMA = @schema AND REFERENCED_TABLE_NAME = @table)
              )
            GROUP BY TABLE_SCHEMA, TABLE_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
            """;

        await using var conn = new MySqlConnection(cs);
        await conn.OpenAsync(ct);
        await using var cmd = new MySqlCommand(sql, conn) { CommandTimeout = timeout };
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await ReadAndSplitAsync(reader, schema, table, ct);
    }

    private static async Task<(List<TableForeignKeyEdge> Outgoing, List<TableForeignKeyEdge> Incoming)> ReadAndSplitAsync(
        System.Data.Common.DbDataReader reader,
        string focusSchema,
        string focusTable,
        CancellationToken ct)
    {
        var outgoing = new List<TableForeignKeyEdge>();
        var incoming = new List<TableForeignKeyEdge>();
        var cmp = StringComparer.OrdinalIgnoreCase;

        while (await reader.ReadAsync(ct))
        {
            var edge = new TableForeignKeyEdge
            {
                ReferencingSchema = reader.GetString(0),
                ReferencingTable = reader.GetString(1),
                ReferencedSchema = reader.GetString(2),
                ReferencedTable = reader.GetString(3),
            };

            var isReferencing = cmp.Equals(edge.ReferencingSchema, focusSchema) &&
                                cmp.Equals(edge.ReferencingTable, focusTable);
            var isReferenced = cmp.Equals(edge.ReferencedSchema, focusSchema) &&
                               cmp.Equals(edge.ReferencedTable, focusTable);

            if (isReferencing)
                outgoing.Add(edge);
            if (isReferenced)
                incoming.Add(edge);
        }

        return (outgoing, incoming);
    }
}
