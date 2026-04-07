using System.Text.Json;
using SqlDataCompare.Mcp;
using Xunit;

namespace SqlDataCompare.Mcp.IntegrationTests;

/// <summary>
/// End-to-end against a real SQL Server when <c>SDC_MCP_INTEGRATION_SQL</c> is set (ADO.NET connection string).
/// Skipped in CI / local runs without the variable.
/// </summary>
public sealed class McpDatabaseToolsSqlIntegrationTests
{
    private const string EnvConnection = "SDC_MCP_INTEGRATION_SQL";

    private static string? ConnectionString => Environment.GetEnvironmentVariable(EnvConnection);

    [SkippableFact]
    public async Task EnumerateTablesJson_returns_non_empty_array()
    {
        var cs = ConnectionString;
        Skip.If(string.IsNullOrWhiteSpace(cs));

        var json = await McpDatabaseTools.EnumerateTablesJsonAsync(cs, "sqlserver", 60, default);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
        Assert.True(doc.RootElement.GetArrayLength() > 0, "Expected at least one table.");
    }

    [SkippableFact]
    public async Task CompareTwoTables_same_database_same_table_reports_identical()
    {
        var cs = ConnectionString;
        Skip.If(string.IsNullOrWhiteSpace(cs));

        var listJson = await McpDatabaseTools.EnumerateTablesJsonAsync(cs, "sqlserver", 60, default);
        var tables = JsonSerializer.Deserialize<List<TableEntry>>(listJson, JsonOpts);
        Skip.If(tables is null || tables.Count == 0);

        foreach (var table in tables.Take(30))
        {
            var resultJson = await McpDatabaseTools.CompareTwoTablesJsonAsync(
                cs,
                cs,
                table.Schema,
                table.Name,
                null,
                null,
                "sqlserver",
                50,
                60,
                logger: null,
                default);

            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            Assert.True(root.TryGetProperty("tables", out var tablesEl));
            Assert.Equal(JsonValueKind.Array, tablesEl.ValueKind);
            Assert.Equal(1, tablesEl.GetArrayLength());
            var t0 = tablesEl[0];
            var status = t0.GetProperty("status").GetString();
            if (string.Equals(status, "error", StringComparison.Ordinal))
                continue;

            Assert.Equal("identical", status);
            Assert.Equal(0, t0.GetProperty("rowsOnlyInSource").GetInt64());
            Assert.Equal(0, t0.GetProperty("rowsOnlyInDestination").GetInt64());
            Assert.Equal(0, t0.GetProperty("rowsWithValueDifferences").GetInt64());
            return;
        }

        Skip.If(true, "No table in the first 30 enumerations compared successfully (e.g. missing PK on all).");
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private sealed class TableEntry
    {
        public string Schema { get; set; } = "";
        public string Name { get; set; } = "";
    }
}
