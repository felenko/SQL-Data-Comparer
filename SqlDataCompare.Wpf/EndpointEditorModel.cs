using CommunityToolkit.Mvvm.ComponentModel;
using SqlDataCompare.Project;

namespace SqlDataCompare.Wpf;

public partial class EndpointEditorModel : ObservableObject
{
    public static string[] Kinds { get; } = { "database", "insertFolder" };
    public static string[] DatabaseProviders { get; } = { "sqlserver", "postgresql", "mysql" };
    public static string[] SqlDialects { get; } = { "SqlServer", "PostgreSql", "MySql" };

    [ObservableProperty] private string kind = "database";

    [ObservableProperty] private string provider = "sqlserver";
    [ObservableProperty] private string connectionString = "";
    [ObservableProperty] private string? schemaIncludePattern;
    [ObservableProperty] private string? connectionDpapiBase64;

    [ObservableProperty] private string folderRootPath = "";
    [ObservableProperty] private string sqlDialect = "SqlServer";
    [ObservableProperty] private string fileNaming = "{table}.sql";
    [ObservableProperty] private string searchPattern = "*.sql";
    [ObservableProperty] private string? defaultSchema;

    public DataEndpoint ToEndpoint()
    {
        if (string.Equals(Kind, "insertFolder", StringComparison.OrdinalIgnoreCase))
        {
            return new InsertFolderEndpoint
            {
                RootPath = FolderRootPath.Trim(),
                SqlDialect = SqlDialect,
                FileNaming = FileNaming,
                SearchPattern = string.IsNullOrWhiteSpace(SearchPattern) ? "*.sql" : SearchPattern,
                DefaultSchema = string.IsNullOrWhiteSpace(DefaultSchema) ? null : DefaultSchema.Trim(),
            };
        }

        return new DatabaseEndpoint
        {
            Provider = Provider,
            ConnectionString = ConnectionString,
            ConnectionStringDpapiBase64 = string.IsNullOrWhiteSpace(ConnectionDpapiBase64) ? null : ConnectionDpapiBase64.Trim(),
            SchemaIncludePattern = string.IsNullOrWhiteSpace(SchemaIncludePattern) ? null : SchemaIncludePattern.Trim(),
        };
    }

    public void ApplyFrom(DataEndpoint endpoint)
    {
        ConnectionDpapiBase64 = null;
        switch (endpoint)
        {
            case DatabaseEndpoint d:
                Kind = "database";
                Provider = d.Provider;
                ConnectionString = d.ConnectionString;
                ConnectionDpapiBase64 = d.ConnectionStringDpapiBase64;
                SchemaIncludePattern = d.SchemaIncludePattern;
                break;
            case InsertFolderEndpoint f:
                Kind = "insertFolder";
                FolderRootPath = f.RootPath;
                SqlDialect = f.SqlDialect;
                FileNaming = f.FileNaming;
                SearchPattern = f.SearchPattern;
                DefaultSchema = f.DefaultSchema;
                break;
        }
    }
}
