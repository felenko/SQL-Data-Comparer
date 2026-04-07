namespace SqlDataCompare.Schema;

public readonly record struct TableRef(string Schema, string Name)
{
    public string Display => string.IsNullOrEmpty(Schema) ? Name : $"{Schema}.{Name}";
}
