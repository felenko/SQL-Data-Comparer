using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Connection;

public static class EndpointConnectionTester
{
    public static async Task TestAsync(DataEndpoint endpoint, CompareOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(options);

        switch (endpoint)
        {
            case DatabaseEndpoint db:
                await TestDatabaseAsync(db, options, cancellationToken).ConfigureAwait(false);
                return;
            case InsertFolderEndpoint folder:
                TestInsertFolder(folder);
                return;
            default:
                throw new ArgumentException("Unknown endpoint type.", nameof(endpoint));
        }
    }

    private static void TestInsertFolder(InsertFolderEndpoint folder)
    {
        var path = folder.RootPath?.Trim() ?? "";
        if (string.IsNullOrEmpty(path))
            throw new InvalidOperationException("Folder path is empty.");
        if (!Directory.Exists(path))
            throw new InvalidOperationException($"Folder does not exist: {path}");
    }

    private static async Task TestDatabaseAsync(DatabaseEndpoint endpoint, CompareOptions options, CancellationToken cancellationToken)
    {
        var cs = DatabaseConnectionResolver.ResolveEffectiveConnectionString(endpoint);
        if (string.IsNullOrWhiteSpace(cs))
            throw new InvalidOperationException("Connection string is empty.");

        var timeout = options.CommandTimeoutSeconds > 0 ? options.CommandTimeoutSeconds : 120;
        var kind = DatabaseProviderNames.Parse(endpoint.Provider);

        switch (kind)
        {
            case DatabaseProviderKind.SqlServer:
                await using (var conn = new SqlConnection(cs))
                {
                    await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandTimeout = timeout;
                    cmd.CommandText = "SELECT 1";
                    _ = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }
                break;

            case DatabaseProviderKind.PostgreSql:
                await using (var conn = new NpgsqlConnection(cs))
                {
                    await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                    await using var cmd = new NpgsqlCommand("SELECT 1", conn) { CommandTimeout = timeout };
                    _ = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }
                break;

            case DatabaseProviderKind.MySql:
                await using (var conn = new MySqlConnection(cs))
                {
                    await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                    await using var cmd = new MySqlCommand("SELECT 1", conn) { CommandTimeout = timeout };
                    _ = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(endpoint), kind, null);
        }
    }
}
