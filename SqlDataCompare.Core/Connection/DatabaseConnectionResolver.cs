using SqlDataCompare.Project;

namespace SqlDataCompare.Connection;

public static class DatabaseConnectionResolver
{
    /// <summary>
    /// Resolves the connection string to open. Non-empty plaintext <see cref="DatabaseEndpoint.ConnectionString"/>
    /// wins over <see cref="DatabaseEndpoint.ConnectionStringDpapiBase64"/> so the connection shown in the UI
    /// (or edited in a project file) is what runs—avoiding stale DPAPI blobs after the user pastes a new string.
    /// </summary>
    public static string ResolveEffectiveConnectionString(DatabaseEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        if (!string.IsNullOrWhiteSpace(endpoint.ConnectionString))
            return ConnectionStringExpander.Expand(endpoint.ConnectionString);

        if (!string.IsNullOrEmpty(endpoint.ConnectionStringDpapiBase64))
        {
            if (!OperatingSystem.IsWindows())
                throw new InvalidOperationException("Project uses DPAPI-protected connection string; unsupported on this OS.");
#pragma warning disable CA1416 // DPAPI Windows-only; guarded by OperatingSystem.IsWindows
            return ConnectionStringExpander.Expand(
                DpapiConnectionString.UnprotectFromBase64(endpoint.ConnectionStringDpapiBase64));
#pragma warning restore CA1416
        }

        return ConnectionStringExpander.Expand(endpoint.ConnectionString);
    }
}
