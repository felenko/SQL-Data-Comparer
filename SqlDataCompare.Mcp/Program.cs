using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

// Stdio MCP server for Cursor / Claude / other MCP clients.
// Cursor: add to MCP config, e.g.
// "mcpServers": {
//   "sql-data-compare": {
//     "command": "dotnet",
//     "args": ["run", "--project", "C:/path/to/SqlDataCompare/SqlDataCompare.Mcp/SqlDataCompare.Mcp.csproj", "--no-build"],
//     "env": {}
//   }
// }
var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddConsole(consoleLogOptions =>
{
    consoleLogOptions.LogToStandardErrorThreshold = LogLevel.Trace;
});
builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();
