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
                AddMissingSample(RowDifferenceKind.MissingInDestination, ks, srcSorted[i], sortKeysSrc, sortKeysDst,
                    valueColumns, ref sample, maxReportedDiffs);
                i++;
            }
            else if (cmp > 0)
            {
                onlyDst++;
                AddMissingSample(RowDifferenceKind.MissingInSource, kd, dstSorted[j], sortKeysSrc, sortKeysDst,
                    valueColumns, ref sample, maxReportedDiffs);
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
            AddMissingSample(RowDifferenceKind.MissingInDestination, KeyString(srcSorted[i], sortKeysSrc), srcSorted[i],
                sortKeysSrc, sortKeysDst, valueColumns, ref sample, maxReportedDiffs);
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
            AddMissingSample(RowDifferenceKind.MissingInSource, KeyString(dstSorted[j], sortKeysDst), dstSorted[j],
                sortKeysSrc, sortKeysDst, valueColumns, ref sample, maxReportedDiffs);
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

    /// <summary>
    /// Adds a sample for a row only on source or only on destination, with every projected column shown
    /// (keys + mapped value columns) so the UI can list full row values on the present side.
    /// </summary>
    private static void AddMissingSample(
        RowDifferenceKind kind,
        string key,
        Dictionary<string, object?> row,
        IReadOnlyList<string> sourceKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valueColumns,
        ref List<RowDifference> list,
        int cap)
    {
        if (list.Count >= cap) return;
        var mismatches = BuildColumnListForMissingRow(kind, row, sourceKeyColumns, destKeyColumns, valueColumns);
        list.Add(new RowDifference
        {
            KeyDisplay = key,
            Kind = kind,
            ColumnMismatches = mismatches,
        });
    }

    /// <summary>
    /// One row per column in the merge projection: for missing-on-destination, source values and placeholder on dest;
    /// for missing-on-source, placeholder on source and destination values.
    /// </summary>
    private static List<ColumnMismatch> BuildColumnListForMissingRow(
        RowDifferenceKind kind,
        Dictionary<string, object?> row,
        IReadOnlyList<string> sourceKeyColumns,
        IReadOnlyList<string> destKeyColumns,
        IReadOnlyList<(string SourceColumn, string DestinationColumn)> valueColumns)
    {
        const string MissingSidePlaceholder = "—";
        var list = new List<ColumnMismatch>();

        if (kind == RowDifferenceKind.MissingInDestination)
        {
            for (var k = 0; k < sourceKeyColumns.Count; k++)
            {
                var sk = sourceKeyColumns[k];
                row.TryGetValue(sk, out var v);
                var dk = k < destKeyColumns.Count ? destKeyColumns[k] : sk;
                var label = string.Equals(sk, dk, StringComparison.Ordinal) ? sk : $"{sk} → {dk}";
                list.Add(new ColumnMismatch
                {
                    Column = label,
                    SourceValue = FormatVal(v),
                    DestinationValue = MissingSidePlaceholder,
                });
            }

            foreach (var (sc, dc) in valueColumns)
            {
                row.TryGetValue(sc, out var v);
                var label = string.Equals(sc, dc, StringComparison.Ordinal) ? sc : $"{sc} → {dc}";
                list.Add(new ColumnMismatch
                {
                    Column = label,
                    SourceValue = FormatVal(v),
                    DestinationValue = MissingSidePlaceholder,
                });
            }
        }
        else
        {
            for (var k = 0; k < destKeyColumns.Count; k++)
            {
                var dk = destKeyColumns[k];
                row.TryGetValue(dk, out var v);
                var sk = k < sourceKeyColumns.Count ? sourceKeyColumns[k] : dk;
                var label = string.Equals(sk, dk, StringComparison.Ordinal) ? dk : $"{sk} → {dk}";
                list.Add(new ColumnMismatch
                {
                    Column = label,
                    SourceValue = MissingSidePlaceholder,
                    DestinationValue = FormatVal(v),
                });
            }

            foreach (var (sc, dc) in valueColumns)
            {
                row.TryGetValue(dc, out var v);
                var label = string.Equals(sc, dc, StringComparison.Ordinal) ? dc : $"{sc} → {dc}";
                list.Add(new ColumnMismatch
                {
                    Column = label,
                    SourceValue = MissingSidePlaceholder,
                    DestinationValue = FormatVal(v),
                });
            }
        }

        return list;
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
