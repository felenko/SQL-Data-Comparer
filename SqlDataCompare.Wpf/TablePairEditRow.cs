using CommunityToolkit.Mvvm.ComponentModel;

namespace SqlDataCompare.Wpf;

public partial class TablePairEditRow : ObservableObject
{
    [ObservableProperty] private string sourceSchema = "dbo";
    [ObservableProperty] private string sourceTable = "";
    [ObservableProperty] private string destSchema = "";
    [ObservableProperty] private string destTable = "";
    [ObservableProperty] private bool skipCompare;

    public SqlDataCompare.Project.TablePairSelection ToSelection() => new()
    {
        SourceSchema = string.IsNullOrWhiteSpace(SourceSchema) ? null : SourceSchema.Trim(),
        SourceTable = SourceTable.Trim(),
        DestSchema = string.IsNullOrWhiteSpace(DestSchema) ? null : DestSchema.Trim(),
        DestTable = string.IsNullOrWhiteSpace(DestTable) ? null : DestTable.Trim(),
        Skip = SkipCompare,
    };

    public static TablePairEditRow FromSelection(SqlDataCompare.Project.TablePairSelection s) => new()
    {
        SourceSchema = s.SourceSchema ?? "",
        SourceTable = s.SourceTable,
        DestSchema = s.DestSchema ?? "",
        DestTable = s.DestTable ?? "",
        SkipCompare = s.Skip,
    };

    public static TablePairEditRow FromOverride(SqlDataCompare.Project.TableOverride o) => new()
    {
        SourceSchema = o.SourceSchema ?? "",
        SourceTable = o.SourceTable,
        DestSchema = o.DestSchema ?? "",
        DestTable = o.DestTable ?? "",
        SkipCompare = o.SkipCompare,
    };
}
