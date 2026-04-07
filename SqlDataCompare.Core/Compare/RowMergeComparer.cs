namespace SqlDataCompare.Compare;

public static class RowMergeComparer
{
    public static TablePairCompareResult Compare(
        IReadOnlyList<Dictionary<string, object?>> sourceRows,
        IReadOnlyList<Dictionary<string, object?>> destinationRows,
        IReadOnlyList<string> keyColumnsSource,
        IReadOnlyList<string> keyColumnsDestination,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valueColumns,
        ValueComparer comparer,
        int maxReportedDiffs,
        bool sampledBecauseLimited,
        string sourceDisplay,
        string destDisplay,
        KeyResolutionSummary? keySummary,
        IProgress<(int Compared, int Total)>? mergeProgress = null,
        CancellationToken cancellationToken = default)
    {
        if (keyColumnsSource.Count != keyColumnsDestination.Count)
            throw new InvalidOperationException("Source and destination key column counts must match.");

        cancellationToken.ThrowIfCancellationRequested();

        var sortKeysSrc = keyColumnsSource.ToList();
        var sortKeysDst = keyColumnsDestination.ToList();
        // Rows usually arrive already ordered (SELECT … ORDER BY keys). Re-sorting is O(n log n) × 2 and dominates large tables.
        // If the stream is already non-decreasing by KeyString (Ordinal), skip sorting.
        var srcSorted = EnsureSortedByKeyString(sourceRows, sortKeysSrc, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        var dstSorted = EnsureSortedByKeyString(destinationRows, sortKeysDst, cancellationToken);

        var mergeTotal = sourceRows.Count + destinationRows.Count;
        mergeProgress?.Report((0, mergeTotal));

        long onlySrc = 0, onlyDst = 0, valueDiffs = 0;
        var sample = new List<RowDifference>();
        var i = 0;
        var j = 0;
        var steps = 0;
        while (i < srcSorted.Count && j < dstSorted.Count)
        {
            if ((++steps & 0x1FFF) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                mergeProgress?.Report((i + j, mergeTotal));
            }
            var ks = KeyString(srcSorted[i], sortKeysSrc);
            var kd = KeyString(dstSorted[j], sortKeysDst);
            var cmp = string.CompareOrdinal(ks, kd);
            if (cmp < 0)
            {
                onlySrc++;
                AddSample(RowDifferenceKind.MissingInDestination, ks, null, ref sample, maxReportedDiffs);
                i++;
            }
            else if (cmp > 0)
            {
                onlyDst++;
                AddSample(RowDifferenceKind.MissingInSource, kd, null, ref sample, maxReportedDiffs);
                j++;
            }
            else
            {
                var mismatches = new List<ColumnMismatch>();
                foreach (var (sc, dc) in valueColumns)
                {
                    srcSorted[i].TryGetValue(sc, out var va);
                    dstSorted[j].TryGetValue(dc, out var vb);
                    if (!comparer.Equal(va, vb))
                    {
                        mismatches.Add(new ColumnMismatch
                        {
                            Column = $"{sc}->{dc}",
                            SourceValue = FormatVal(va),
                            DestinationValue = FormatVal(vb),
                        });
                    }
                }

                if (mismatches.Count > 0)
                {
                    valueDiffs++;
                    if (sample.Count < maxReportedDiffs)
                    {
                        sample.Add(new RowDifference
                        {
                            KeyDisplay = ks,
                            Kind = RowDifferenceKind.ValueMismatch,
                            ColumnMismatches = mismatches,
                        });
                    }
                }

                i++;
                j++;
            }
        }

        while (i < srcSorted.Count)
        {
            if ((++steps & 0x1FFF) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                mergeProgress?.Report((i + j, mergeTotal));
            }
            onlySrc++;
            AddSample(RowDifferenceKind.MissingInDestination, KeyString(srcSorted[i], sortKeysSrc), null, ref sample,
                maxReportedDiffs);
            i++;
        }

        while (j < dstSorted.Count)
        {
            if ((++steps & 0x1FFF) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                mergeProgress?.Report((i + j, mergeTotal));
            }
            onlyDst++;
            AddSample(RowDifferenceKind.MissingInSource, KeyString(dstSorted[j], sortKeysDst), null, ref sample,
                maxReportedDiffs);
            j++;
        }

        mergeProgress?.Report((i + j, mergeTotal));

        var different = onlySrc > 0 || onlyDst > 0 || valueDiffs > 0;
        var status = !different
            ? TableCompareStatus.Identical
            : sampledBecauseLimited
                ? TableCompareStatus.SampledDifferent
                : TableCompareStatus.Different;

        return new TablePairCompareResult
        {
            SourceTable = sourceDisplay,
            DestinationTable = destDisplay,
            Status = status,
            RowsOnlyInSource = onlySrc,
            RowsOnlyInDestination = onlyDst,
            RowsWithValueDifferences = valueDiffs,
            Sampled = sampledBecauseLimited || sample.Count >= maxReportedDiffs,
            SampleDiffs = sample,
            Keys = keySummary,
        };
    }

    /// <summary>
    /// Returns a list sorted by <see cref="KeyString"/> using <see cref="StringComparer.Ordinal"/>.
    /// When input is already in that order (typical after SQL ORDER BY on key columns), avoids O(n log n) sort.
    /// </summary>
    internal static List<Dictionary<string, object?>> EnsureSortedByKeyString(
        IReadOnlyList<Dictionary<string, object?>> rows,
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        if (rows.Count <= 1)
            return rows is List<Dictionary<string, object?>> l0 ? l0 : rows.ToList();

        string? prev = null;
        for (var i = 0; i < rows.Count; i++)
        {
            if ((i & 0x1FFF) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            var k = KeyString(rows[i], keys);
            if (prev is not null && string.CompareOrdinal(prev, k) > 0)
                return rows.OrderBy(r => KeyString(r, keys), StringComparer.Ordinal).ToList();
            prev = k;
        }

        return rows is List<Dictionary<string, object?>> l ? l : rows.ToList();
    }

    private static void AddSample(
        RowDifferenceKind kind,
        string key,
        IReadOnlyList<ColumnMismatch>? mismatches,
        ref List<RowDifference> list,
        int cap)
    {
        if (list.Count >= cap) return;
        list.Add(new RowDifference
        {
            KeyDisplay = key,
            Kind = kind,
            ColumnMismatches = mismatches,
        });
    }

    private static string KeyString(Dictionary<string, object?> row, IReadOnlyList<string> keys)
    {
        return string.Join('\u001F', keys.Select(k =>
            row.TryGetValue(k, out var v) ? v?.ToString() ?? "\u0000NULL" : "\u0000MISSING"));
    }

    private static string? FormatVal(object? v) => v switch
    {
        null => null,
        byte or short or int or long => v.ToString(),
        decimal d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
        double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
        float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
        DateTimeOffset dx => dx.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
        bool b => b.ToString(),
        _ => v.ToString(),
    };
}
