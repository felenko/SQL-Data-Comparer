using SqlDataCompare.Project;

namespace SqlDataCompare.Tests;

public class ProjectSerializationTests
{
    [Fact]
    public void RoundTripsPolymorphicEndpoints()
    {
        var p = new CompareProject
        {
            Name = "t",
            Source = new DatabaseEndpoint { Provider = "postgresql", ConnectionString = "Host=localhost" },
            Destination = new InsertFolderEndpoint
            {
                RootPath = "C:\\data",
                SqlDialect = "SqlServer",
                FileNaming = "{table}.sql",
            },
            TableOverrides =
            {
                new TableOverride
                {
                    SourceTable = "Orders",
                    KeyColumns = new List<string> { "Id" },
                    WhereClause = "Active = 1",
                },
            },
        };
        var path = Path.Combine(Path.GetTempPath(), "sdc-test-" + Guid.NewGuid() + ".json");
        try
        {
            CompareProjectSerializer.Write(path, p);
            var back = CompareProjectSerializer.Read(path);
            Assert.IsType<DatabaseEndpoint>(back.Source);
            Assert.IsType<InsertFolderEndpoint>(back.Destination);
            Assert.Single(back.TableOverrides);
        }
        finally
        {
            try { File.Delete(path); } catch { /* ignore */ }
        }
    }

    [Fact]
    public void RoundTripsTableSkipFlags()
    {
        var p = new CompareProject
        {
            Source = new DatabaseEndpoint { ConnectionString = "x" },
            Destination = new DatabaseEndpoint { ConnectionString = "y" },
            TablesToCompare =
            {
                new TablePairSelection { SourceTable = "A", Skip = true },
                new TablePairSelection { SourceTable = "B" },
            },
            TableOverrides =
            {
                new TableOverride { SourceTable = "Huge", SkipCompare = true },
            },
        };
        var path = Path.Combine(Path.GetTempPath(), "sdc-skip-" + Guid.NewGuid() + ".json");
        try
        {
            CompareProjectSerializer.Write(path, p);
            var back = CompareProjectSerializer.Read(path);
            Assert.Equal(2, back.TablesToCompare.Count);
            Assert.True(back.TablesToCompare[0].Skip);
            Assert.False(back.TablesToCompare[1].Skip);
            Assert.True(back.TableOverrides[0].SkipCompare);
        }
        finally
        {
            try { File.Delete(path); } catch { /* ignore */ }
        }
    }
}
