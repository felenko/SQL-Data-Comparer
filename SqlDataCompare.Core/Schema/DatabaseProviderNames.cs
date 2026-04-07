namespace SqlDataCompare.Schema;

public static class DatabaseProviderNames
{
    public static DatabaseProviderKind Parse(string? provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
            return DatabaseProviderKind.SqlServer;
        return provider.Trim().ToLowerInvariant() switch
        {
            "sqlserver" or "mssql" or "microsoft" => DatabaseProviderKind.SqlServer,
            "postgresql" or "postgres" or "pg" => DatabaseProviderKind.PostgreSql,
            "mysql" or "mariadb" => DatabaseProviderKind.MySql,
            _ => throw new ArgumentException($"Unknown database provider: {provider}", nameof(provider)),
        };
    }

    public static string ToEndpointString(DatabaseProviderKind kind) => kind switch
    {
        DatabaseProviderKind.SqlServer => "sqlserver",
        DatabaseProviderKind.PostgreSql => "postgresql",
        DatabaseProviderKind.MySql => "mysql",
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };
}
