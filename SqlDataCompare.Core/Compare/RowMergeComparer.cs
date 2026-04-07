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
        KeyResolutionSummary? keySummary)
    {
        if (keyColumnsSource.Count != keyColumnsDestination.Count)
            throw new InvalidOperationException("Source and destination key column counts must match.");

        var sortKeysSrc = keyColumnsSource.ToList();
        var sortKeysDst = keyColumnsDestination.ToList();
        var srcSorted = sourceRows.OrderBy(r => KeyString(r, sortKeysSrc), StringComparer.Ordinal).ToList();
        var dstSorted = destinationRows.OrderBy(r => KeyString(r, sortKeysDst), StringComparer.Ordinal).ToList();

        long onlySrc = 0, onlyDst = 0, valueDiffs = 0;
        var sample = new List<RowDifference>();
        var i = 0;
        var j = 0;
        while (i < srcSorted.Count && j < dstSorted.Count)
        {
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
            onlySrc++;
            AddSample(RowDifferenceKind.MissingInDestination, KeyString(srcSorted[i], sortKeysSrc), null, ref sample,
                maxReportedDiffs);
            i++;
        }

        while (j < dstSorted.Count)
        {
            onlyDst++;
            AddSample(RowDifferenceKind.MissingInSource, KeyString(dstSorted[j], sortKeysDst), null, ref sample,
                maxReportedDiffs);
            j++;
        }

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
