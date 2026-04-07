namespace SqlDataCompare.Schema;

public sealed class TableSchema
{
    public required TableRef Table { get; init; }
    public required IReadOnlyList<ColumnDefinition> Columns { get; init; }
    public IReadOnlyList<string>? PrimaryKeyColumns { get; init; }
}
