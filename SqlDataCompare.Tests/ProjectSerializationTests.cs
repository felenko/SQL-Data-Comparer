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
}
