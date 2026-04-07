using SqlDataCompare.Sql;

namespace SqlDataCompare.Tests;

public class InsertSqlParserTests
{
    [Fact]
    public void ParsesSqlServerInsertWithColumnList()
    {
        var sql = "INSERT INTO [dbo].[T] ([Id], [Name]) VALUES (1, N'x'), (2, 'y');";
        var r = InsertSqlParser.Parse(sql, InsertSqlDialect.SqlServer, null);
        Assert.Equal(2, r.Rows.Count);
        Assert.Equal(1L, r.Rows[0]["Id"]);
        Assert.Equal("x", r.Rows[0]["Name"]);
        Assert.Equal(2L, r.Rows[1]["Id"]);
        Assert.Equal("y", r.Rows[1]["Name"]);
    }

    [Fact]
    public void RowMergeDetectsDifference()
    {
        var src = new List<Dictionary<string, object?>>
        {
            new() { ["Id"] = 1L, ["Name"] = "a" },
        };
        var dst = new List<Dictionary<string, object?>>
        {
            new() { ["Id"] = 1L, ["Name"] = "b" },
        };
        var vc = new Compare.ValueComparer(ordinalIgnoreCase: true, trimStrings: false);
        var result = Compare.RowMergeComparer.Compare(
            src,
            dst,
            ["Id"],
            ["Id"],
            [("Name", "Name")],
            vc,
            maxReportedDiffs: 10,
            sampledBecauseLimited: false,
            "s",
            "d",
            null);
        Assert.Equal(Compare.TableCompareStatus.Different, result.Status);
        Assert.Equal(1, result.RowsWithValueDifferences);
    }
}
