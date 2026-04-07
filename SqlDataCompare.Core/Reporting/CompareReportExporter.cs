using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlDataCompare.Compare;

namespace SqlDataCompare.Reporting;

public static class CompareReportExporter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public static void WriteJson(string path, ProjectCompareResult result) =>
        File.WriteAllText(path, JsonSerializer.Serialize(result, JsonOptions));

    public static void WriteHtml(string path, ProjectCompareResult result)
    {
        var sb = new StringBuilder();
        sb.AppendLine("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>SQL Data Compare</title>");
        sb.AppendLine("<style>body{font-family:system-ui,Segoe UI,sans-serif;margin:24px;}table{border-collapse:collapse;}th,td{border:1px solid #ccc;padding:6px 10px;text-align:left;}tr:nth-child(even){background:#f6f8fa;}.err{color:#b00;}.ok{color:#060;}</style>");
        sb.AppendLine("</head><body>");
        sb.AppendLine($"<h1>{System.Security.SecurityElement.Escape(result.ProjectName)}</h1>");
        sb.AppendLine("<table><thead><tr><th>Source</th><th>Destination</th><th>Status</th><th>Only src</th><th>Only dst</th><th>Value diffs</th><th>Note</th></tr></thead><tbody>");
        foreach (var t in result.Tables)
        {
            var cls = t.Status == TableCompareStatus.Identical ? "ok" : t.Status == TableCompareStatus.Error ? "err" : "";
            sb.AppendLine($"<tr class=\"{cls}\"><td>{Esc(t.SourceTable)}</td><td>{Esc(t.DestinationTable)}</td><td>{t.Status}</td>");
            sb.AppendLine($"<td>{t.RowsOnlyInSource}</td><td>{t.RowsOnlyInDestination}</td><td>{t.RowsWithValueDifferences}</td>");
            sb.AppendLine($"<td>{Esc(t.ErrorMessage)}</td></tr>");
        }
        sb.AppendLine("</tbody></table></body></html>");
        File.WriteAllText(path, sb.ToString());
    }

    public static void WriteCsv(string path, ProjectCompareResult result)
    {
        var sb = new StringBuilder();
        sb.AppendLine("source_table,destination_table,status,only_source,only_destination,value_diffs,error");
        foreach (var t in result.Tables)
        {
            sb.AppendLine(string.Join(",",
                Csv(t.SourceTable),
                Csv(t.DestinationTable),
                t.Status.ToString(),
                t.RowsOnlyInSource,
                t.RowsOnlyInDestination,
                t.RowsWithValueDifferences,
                Csv(t.ErrorMessage)));
        }

        File.WriteAllText(path, sb.ToString());
    }

    public static void WriteJUnit(string path, ProjectCompareResult result)
    {
        var failures = result.Tables.Count(t => t.Status is TableCompareStatus.Different or TableCompareStatus.SampledDifferent or TableCompareStatus.Error);
        var sb = new StringBuilder();
        sb.AppendLine($"<?xml version=\"1.0\" encoding=\"utf-8\"?>");
        sb.AppendLine($"<testsuite name=\"SqlDataCompare\" tests=\"{result.Tables.Count}\" failures=\"{failures}\" errors=\"0\">");
        foreach (var t in result.Tables)
        {
            var name = $"{t.SourceTable} -> {t.DestinationTable}";
            sb.AppendLine($"  <testcase name=\"{XmlAttr(name)}\" classname=\"TableCompare\">");
            if (t.Status is TableCompareStatus.Different or TableCompareStatus.SampledDifferent)
            {
                var msg = $"onlySrc={t.RowsOnlyInSource} onlyDst={t.RowsOnlyInDestination} valueDiffs={t.RowsWithValueDifferences}";
                sb.AppendLine($"    <failure message=\"{XmlAttr(msg)}\"/>");
            }
            else if (t.Status == TableCompareStatus.Error && !string.IsNullOrEmpty(t.ErrorMessage))
            {
                sb.AppendLine($"    <failure message=\"{XmlAttr(t.ErrorMessage)}\"/>");
            }

            sb.AppendLine("  </testcase>");
        }

        sb.AppendLine("</testsuite>");
        File.WriteAllText(path, sb.ToString());
    }

    private static string Esc(string? s) => System.Security.SecurityElement.Escape(s ?? "") ?? "";

    private static string Csv(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.Contains('"') || s.Contains(',') || s.Contains('\n'))
            return $"\"{s.Replace("\"", "\"\"")}\"";
        return s;
    }

    private static string XmlAttr(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return s.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;");
    }
}
