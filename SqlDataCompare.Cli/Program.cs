using Microsoft.Extensions.Logging;
using SqlDataCompare.Compare;
using SqlDataCompare.Project;
using SqlDataCompare.Reporting;

static void WriteUsage()
{
    Console.WriteLine("""
        SqlDataCompare.Cli — compare data across databases or INSERT script folders.

        Usage:
          SqlDataCompare.Cli --project <path> [--report <json>] [--export-html <path>] [--export-csv <path>] [--export-junit <path>] [--verbosity Information|Warning|...]

        Exit codes: 0 identical, 1 error, 2 differences detected.
        """);
}

static LogLevel ParseLogLevel(string? v) =>
    v switch
    {
        null or "" => LogLevel.Information,
        "Trace" => LogLevel.Trace,
        "Debug" => LogLevel.Debug,
        "Information" => LogLevel.Information,
        "Warning" => LogLevel.Warning,
        "Error" => LogLevel.Error,
        "Critical" => LogLevel.Critical,
        _ => Enum.TryParse<LogLevel>(v, ignoreCase: true, out var l) ? l : LogLevel.Information,
    };

if (args.Length == 0)
{
    WriteUsage();
    return 1;
}

if (args.Any(a => a.Equals("--help", StringComparison.OrdinalIgnoreCase) || a.Equals("-h", StringComparison.OrdinalIgnoreCase)))
{
    WriteUsage();
    return 0;
}

string? projectPath = null;
string? reportJson = null;
string? html = null;
string? csv = null;
string? junit = null;
LogLevel verbosity = LogLevel.Information;

for (var i = 0; i < args.Length; i++)
{
    var a = args[i];
    if (a.Equals("--project", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        projectPath = args[++i];
    else if (a.Equals("--report", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        reportJson = args[++i];
    else if (a.Equals("--export-html", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        html = args[++i];
    else if (a.Equals("--export-csv", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        csv = args[++i];
    else if (a.Equals("--export-junit", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        junit = args[++i];
    else if ((a.Equals("--verbosity", StringComparison.OrdinalIgnoreCase) || a.Equals("-v", StringComparison.OrdinalIgnoreCase)) &&
             i + 1 < args.Length)
        verbosity = ParseLogLevel(args[++i]);
}

if (string.IsNullOrWhiteSpace(projectPath) || !File.Exists(projectPath))
{
    Console.Error.WriteLine("Provide a valid --project path to a JSON project file.");
    WriteUsage();
    return 1;
}

using var loggerFactory = LoggerFactory.Create(b =>
{
    b.AddSimpleConsole(o =>
    {
        o.SingleLine = true;
        o.TimestampFormat = "HH:mm:ss ";
    });
    b.SetMinimumLevel(verbosity);
});
var logger = loggerFactory.CreateLogger("SqlDataCompare.Cli");

CompareProject project;
try
{
    project = CompareProjectSerializer.Read(projectPath);
}
catch (Exception ex)
{
    logger.LogError(ex, "Failed to read project file.");
    return 1;
}

try
{
    var svc = new DataCompareService();
    var result = await svc.RunAsync(project, logger, CancellationToken.None);

    if (reportJson is not null)
        CompareReportExporter.WriteJson(reportJson, result);
    if (html is not null)
        CompareReportExporter.WriteHtml(html, result);
    if (csv is not null)
        CompareReportExporter.WriteCsv(csv, result);
    if (junit is not null)
        CompareReportExporter.WriteJUnit(junit, result);

    foreach (var t in result.Tables)
    {
        if (t.Status == TableCompareStatus.Identical)
            logger.LogInformation("OK {Src} ↔ {Dst}", t.SourceTable, t.DestinationTable);
        else if (t.Status == TableCompareStatus.Skipped)
            logger.LogWarning("SKIP {Src} ↔ {Dst}: {Msg}", t.SourceTable, t.DestinationTable, t.ErrorMessage);
        else if (t.Status == TableCompareStatus.Error)
            logger.LogError("ERR {Src} ↔ {Dst}: {Msg}", t.SourceTable, t.DestinationTable, t.ErrorMessage);
        else
            logger.LogWarning(
                "DIFF {Src} ↔ {Dst}: +src={A} +dst={B} valueDiffs={C}",
                t.SourceTable, t.DestinationTable, t.RowsOnlyInSource, t.RowsOnlyInDestination, t.RowsWithValueDifferences);
    }

    if (result.Tables.Any(t => t.Status == TableCompareStatus.Error))
        return 1;
    if (result.Tables.Any(t => t.Status is TableCompareStatus.Different or TableCompareStatus.SampledDifferent))
        return 2;
    return 0;
}
catch (Exception ex)
{
    logger.LogError(ex, "Compare failed.");
    return 1;
}
