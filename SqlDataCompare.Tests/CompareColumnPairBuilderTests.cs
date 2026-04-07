using SqlDataCompare.Project;
using SqlDataCompare.Schema;

namespace SqlDataCompare.Tests;

public class CompareColumnPairBuilderTests
{
    private static TableSchema Schema(string table, params ColumnDefinition[] cols) => new()
    {
        Table = new TableRef("dbo", table),
        Columns = cols,
        PrimaryKeyColumns = ["Id"],
    };

    [Theory]
    [InlineData("varbinary")]
    [InlineData("image")]
    [InlineData("bytea")]
    [InlineData("longblob")]
    public void BinaryHeuristics_recognizes_types(string dataType) =>
        Assert.True(BinaryColumnHeuristics.IsBinaryLikeType(dataType));

    [Fact]
    public void Skips_binary_columns_when_option_on()
    {
        var src = Schema("T",
            new ColumnDefinition { Name = "Id", PhysicalType = "int", IsNullable = false, IsIdentity = false },
            new ColumnDefinition { Name = "Name", PhysicalType = "nvarchar", IsNullable = true, IsIdentity = false },
            new ColumnDefinition { Name = "Pic", PhysicalType = "varbinary", IsNullable = true, IsIdentity = false });
        var dst = Schema("T",
            new ColumnDefinition { Name = "Id", PhysicalType = "int", IsNullable = false, IsIdentity = false },
            new ColumnDefinition { Name = "Name", PhysicalType = "nvarchar", IsNullable = true, IsIdentity = false },
            new ColumnDefinition { Name = "Pic", PhysicalType = "varbinary", IsNullable = true, IsIdentity = false });

        var withSkip = CompareColumnPairBuilder.BuildValueColumnPairs(
            src, dst, null, ["Id"], StringComparer.OrdinalIgnoreCase, skipBinaryLikeColumns: true);
        Assert.Equal([("Name", "Name")], withSkip);

        var noSkip = CompareColumnPairBuilder.BuildValueColumnPairs(
            src, dst, null, ["Id"], StringComparer.OrdinalIgnoreCase, skipBinaryLikeColumns: false);
        Assert.Equal([("Name", "Name"), ("Pic", "Pic")], noSkip);
    }
}
