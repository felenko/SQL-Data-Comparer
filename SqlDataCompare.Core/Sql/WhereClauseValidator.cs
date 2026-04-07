namespace SqlDataCompare.Sql;

public static class WhereClauseValidator
{
    public static bool IsPermitted(string? clause)
    {
        if (string.IsNullOrWhiteSpace(clause))
            return true;
        var t = clause.Trim();
        if (t.Contains(';', StringComparison.Ordinal))
            return false;
        if (t.Contains("--", StringComparison.Ordinal))
            return false;
        if (t.Contains("/*", StringComparison.Ordinal))
            return false;
        if (t.IndexOfAny(['\r', '\n']) >= 0)
            return false;
        if (t.StartsWith("EXEC", StringComparison.OrdinalIgnoreCase))
            return false;
        return true;
    }
}
