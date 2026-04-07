namespace SqlDataCompare.Sql;

public static class InsertSqlDialectParser
{
    public static InsertSqlDialect Parse(string? dialect)
    {
        if (string.IsNullOrWhiteSpace(dialect))
            return InsertSqlDialect.SqlServer;
        return dialect.Trim().ToLowerInvariant() switch
        {
            "sqlserver" or "mssql" => InsertSqlDialect.SqlServer,
            "postgresql" or "postgres" or "pg" => InsertSqlDialect.PostgreSql,
            "mysql" or "mariadb" => InsertSqlDialect.MySql,
            _ => throw new ArgumentException($"Unknown SQL dialect: {dialect}", nameof(dialect)),
        };
    }
}
