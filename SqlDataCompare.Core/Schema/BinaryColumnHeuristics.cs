namespace SqlDataCompare.Schema;

/// <summary>Detects physical types that are expensive or inappropriate for row-level value compare (blobs, raw binary).</summary>
public static class BinaryColumnHeuristics
{
    /// <summary>Returns true when this column should be omitted from compare projections when skipping is enabled.</summary>
    public static bool IsBinaryLikeType(string? physicalType)
    {
        if (string.IsNullOrWhiteSpace(physicalType))
            return false;

        var t = physicalType.Trim().ToLowerInvariant();

        // SQL Server: image, binary, varbinary (incl. (max) — DATA_TYPE is still "varbinary")
        if (t is "image" or "varbinary" or "binary")
            return true;

        // PostgreSQL
        if (t is "bytea")
            return true;

        // MySQL / MariaDB DATA_TYPE
        if (t is "blob" or "tinyblob" or "mediumblob" or "longblob" or "binary" or "varbinary")
            return true;

        // Defensive: some providers return composite names
        if (t.Contains("blob", StringComparison.OrdinalIgnoreCase))
            return true;
        if (t.StartsWith("varbinary", StringComparison.OrdinalIgnoreCase))
            return true;
        if (t.StartsWith("binary(", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }
}
