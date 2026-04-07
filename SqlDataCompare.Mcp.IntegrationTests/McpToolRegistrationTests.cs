using System.Text.RegularExpressions;
using Xunit;

namespace SqlDataCompare.Mcp.IntegrationTests;

/// <summary>
/// No database, no reference to the MCP exe (avoids rebuild/copy conflicts when SqlDataCompare.Mcp is running).
/// Asserts the MCP tools source still exposes five SDK tools.
/// </summary>
public sealed class McpToolRegistrationTests
{
    [Fact]
    public void Mcp_tools_source_defines_five_McpServerTool_methods()
    {
        var root = RepoRootResolver.Resolve();
        var path = Path.Combine(root, "SqlDataCompare.Mcp", "SqlDataCompareMcpTools.cs");
        Assert.True(File.Exists(path), $"Expected {path}.");

        var text = File.ReadAllText(path);
        var matches = Regex.Matches(text, @"\[McpServerTool\]");
        Assert.Equal(5, matches.Count);

        Assert.Contains("SqlEnumerateTables", text, StringComparison.Ordinal);
        Assert.Contains("SqlGetTableDataWithRelations", text, StringComparison.Ordinal);
        Assert.Contains("SqlCompareTwoTables", text, StringComparison.Ordinal);
        Assert.Contains("SqlCopySelectedRowsToDestination", text, StringComparison.Ordinal);
        Assert.Contains("SqlCopyProjectDifferencesToDestination", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Mcp_tools_type_is_marked_for_discovery_in_source()
    {
        var root = RepoRootResolver.Resolve();
        var path = Path.Combine(root, "SqlDataCompare.Mcp", "SqlDataCompareMcpTools.cs");
        var text = File.ReadAllText(path);
        Assert.Contains("[McpServerToolType]", text, StringComparison.Ordinal);
    }
}

internal static class RepoRootResolver
{
    internal static string Resolve()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "SqlDataCompare.sln")))
                return dir.FullName;
            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            "Could not locate repository root (SqlDataCompare.sln). Run tests from the built output under the repo.");
    }
}
