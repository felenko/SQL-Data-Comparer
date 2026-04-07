namespace SqlDataCompare.Schema;

public sealed class ColumnDefinition
{
    public required string Name { get; init; }
    public required string PhysicalType { get; init; }
    public bool IsNullable { get; init; }
    public bool IsIdentity { get; init; }
}
