namespace SqlDataCompare.Schema;

public sealed class ColumnDefinition
{
    public required string Name { get; init; }
    /// <summary>Provider catalog type (e.g. INFORMATION_SCHEMA.DATA_TYPE).</summary>
    public required string PhysicalType { get; init; }
    public bool IsNullable { get; init; }
    public bool IsIdentity { get; init; }
}
