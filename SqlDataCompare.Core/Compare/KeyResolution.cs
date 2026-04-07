using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Compare;

public enum KeyConfidence
{
    UserOverride,
    CatalogPrimaryKey,
    SingleIdentityColumn,
    HeuristicColumnName,
    Ambiguous,
    None,
}

public sealed class KeyResolutionResult
{
    public required KeyConfidence Confidence { get; init; }
    public required IReadOnlyList<string> KeyColumns { get; init; }
    public string? Detail { get; init; }
}

public static class PrimaryKeyResolver
{
    public static KeyResolutionResult Resolve(TableSchema schema, TableOverride? tableOverride, IEqualityComparer<string> nameComparer)
    {
        if (tableOverride?.KeyColumns is { Count: > 0 } kc)
        {
            return new KeyResolutionResult
            {
                Confidence = KeyConfidence.UserOverride,
                KeyColumns = kc,
                Detail = "User-specified key columns in project file.",
            };
        }

        if (schema.PrimaryKeyColumns is { Count: > 0 } pk)
        {
            return new KeyResolutionResult
            {
                Confidence = KeyConfidence.CatalogPrimaryKey,
                KeyColumns = pk,
                Detail = "Primary key from database metadata.",
            };
        }

        var idCols = schema.Columns.Where(c => c.IsIdentity).Select(c => c.Name).ToList();
        if (idCols.Count == 1)
        {
            return new KeyResolutionResult
            {
                Confidence = KeyConfidence.SingleIdentityColumn,
                KeyColumns = idCols,
                Detail = "Single identity / serial column.",
            };
        }

        var tableName = schema.Table.Name;
        var candidates = new List<string>();
        foreach (var c in schema.Columns)
        {
            if (LooksLikeIdColumn(c.Name, tableName, nameComparer))
                candidates.Add(c.Name);
        }

        if (candidates.Count == 1)
        {
            return new KeyResolutionResult
            {
                Confidence = KeyConfidence.HeuristicColumnName,
                KeyColumns = candidates,
                Detail = "Heuristic: single Id / {Table}Id style column.",
            };
        }

        if (candidates.Count > 1)
        {
            return new KeyResolutionResult
            {
                Confidence = KeyConfidence.Ambiguous,
                KeyColumns = Array.Empty<string>(),
                Detail = $"Multiple heuristic key candidates: {string.Join(", ", candidates)}.",
            };
        }

        return new KeyResolutionResult
        {
            Confidence = KeyConfidence.None,
            KeyColumns = Array.Empty<string>(),
            Detail = "No primary key, identity column, or Id-style column detected.",
        };
    }

    private static bool LooksLikeIdColumn(string columnName, string tableName, IEqualityComparer<string> comparer)
    {
        if (comparer.Equals(columnName, "Id"))
            return true;
        if (comparer.Equals(columnName, $"{tableName}Id"))
            return true;
        if (comparer.Equals(columnName, $"{tableName}_id"))
            return true;
        return false;
    }
}
