using SqlDataCompare.Project;

namespace SqlDataCompare.Schema;

/// <summary>Builds source/destination column pairs for compare and sync value projection.</summary>
public static class CompareColumnPairBuilder
{
    public static List<(string SourceColumn, string DestinationColumn)> BuildValueColumnPairs(
        TableSchema src,
        TableSchema dst,
        TableOverride? ov,
        IReadOnlyList<string> sourceKeyColumns,
        StringComparer comparer,
        bool skipBinaryLikeColumns)
    {
        var ignore = new HashSet<string>(ov?.IgnoreColumns ?? Enumerable.Empty<string>(), comparer);
        var map = ov?.ColumnMap;
        var keys = new HashSet<string>(sourceKeyColumns, comparer);
        var pairs = new List<(string, string)>();

        if (map is not null && map.Count > 0)
        {
            foreach (var (sc, dc) in map)
            {
                if (ignore.Contains(sc)) continue;
                if (keys.Contains(sc)) continue;
                var srcCol = src.Columns.FirstOrDefault(c => comparer.Equals(c.Name, sc));
                var dstCol = dst.Columns.FirstOrDefault(c => comparer.Equals(c.Name, dc));
                if (srcCol is null || dstCol is null) continue;
                if (skipBinaryLikeColumns &&
                    (BinaryColumnHeuristics.IsBinaryLikeType(srcCol.PhysicalType) ||
                     BinaryColumnHeuristics.IsBinaryLikeType(dstCol.PhysicalType)))
                    continue;
                pairs.Add((srcCol.Name, dstCol.Name));
            }
        }

        foreach (var scol in src.Columns)
        {
            if (ignore.Contains(scol.Name)) continue;
            if (keys.Contains(scol.Name)) continue;
            var dcol = dst.Columns.FirstOrDefault(d => comparer.Equals(d.Name, scol.Name));
            if (dcol is null) continue;
            if (pairs.Any(p => comparer.Equals(p.Item1, scol.Name))) continue;
            if (skipBinaryLikeColumns &&
                (BinaryColumnHeuristics.IsBinaryLikeType(scol.PhysicalType) ||
                 BinaryColumnHeuristics.IsBinaryLikeType(dcol.PhysicalType)))
                continue;
            pairs.Add((scol.Name, dcol.Name));
        }

        return pairs;
    }
}
