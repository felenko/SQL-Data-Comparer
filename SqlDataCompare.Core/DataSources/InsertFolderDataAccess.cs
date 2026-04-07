using SqlDataCompare.Project;
using SqlDataCompare.Schema;
using SqlDataCompare.Sql;

namespace SqlDataCompare.DataSources;

public sealed class InsertFolderDataAccess
{
    private readonly InsertFolderEndpoint _endpoint;
    private readonly InsertSqlDialect _dialect;

    public InsertFolderDataAccess(InsertFolderEndpoint endpoint)
    {
        _endpoint = endpoint;
        _dialect = InsertSqlDialectParser.Parse(endpoint.SqlDialect);
    }

    public IReadOnlyList<TableRef> ListTables()
    {
        var root = Path.GetFullPath(_endpoint.RootPath);
        if (!Directory.Exists(root))
            throw new DirectoryNotFoundException($"INSERT folder not found: {root}");
        var pattern = string.IsNullOrWhiteSpace(_endpoint.SearchPattern) ? "*.sql" : _endpoint.SearchPattern;
        var files = Directory.GetFiles(root, pattern, SearchOption.TopDirectoryOnly);
        var tables = new List<TableRef>();
        foreach (var file in files)
        {
            var name = Path.GetFileName(file);
            if (InsertFolderNaming.TryParseTableFromFileName(name, _endpoint.FileNaming, _endpoint.DefaultSchema, _dialect, out var table))
                tables.Add(table);
        }

        return tables.OrderBy(t => t.Schema, StringComparer.Ordinal).ThenBy(t => t.Name, StringComparer.Ordinal).ToList();
    }

    public TableSchema BuildSchemaFromInserts(TableRef table, IReadOnlyList<ColumnDefinition>? priorColumns = null)
    {
        var fallbackNames = priorColumns?.Select(c => c.Name).ToList();
        var rows = LoadAllRows(table, null, null, fallbackNames);
        if (rows.Count == 0)
        {
            var cols = priorColumns?.ToList() ?? new List<ColumnDefinition>();
            return new TableSchema
            {
                Table = table,
                Columns = cols,
                PrimaryKeyColumns = Array.Empty<string>(),
            };
        }

        var names = rows[0].Keys.ToList();
        var definitions = priorColumns?.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        var list = new List<ColumnDefinition>();
        foreach (var n in names)
        {
            if (definitions != null && definitions.TryGetValue(n, out var def))
            {
                list.Add(def);
                continue;
            }

            var sample = rows.Select(r => r.TryGetValue(n, out var v) ? v : null).FirstOrDefault(v => v is not null);
            list.Add(new ColumnDefinition
            {
                Name = n,
                PhysicalType = sample?.GetType().Name ?? "unknown",
                IsNullable = rows.Any(r => !r.ContainsKey(n) || r[n] is null),
                IsIdentity = false,
            });
        }

        return new TableSchema
        {
            Table = table,
            Columns = list,
            PrimaryKeyColumns = Array.Empty<string>(),
        };
    }

    public List<Dictionary<string, object?>> LoadAllRows(
        TableRef table,
        string? explicitInsertPath,
        int? maxRows,
        IReadOnlyList<string>? fallbackColumnOrder)
    {
        var root = Path.GetFullPath(_endpoint.RootPath);
        var path = !string.IsNullOrWhiteSpace(explicitInsertPath)
            ? Path.GetFullPath(explicitInsertPath)
            : Path.Combine(root, InsertFolderNaming.FileNameForTable(table, _endpoint.FileNaming));
        if (!File.Exists(path))
            throw new FileNotFoundException("INSERT file not found for table.", path);
        var sql = File.ReadAllText(path);
        var dialect = _dialect;
        var batches = SplitInsertBatches(sql);
        var all = new List<Dictionary<string, object?>>();
        List<string>? fallback = fallbackColumnOrder?.ToList();
        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (trimmed.Length == 0) continue;
            var parsed = InsertSqlParser.Parse(trimmed, dialect, fallback);
            if (parsed.ExplicitColumnNames is { Count: > 0 })
                fallback = parsed.ExplicitColumnNames;
            all.AddRange(parsed.Rows);
            if (maxRows is > 0 && all.Count >= maxRows.Value)
            {
                all = all.Take(maxRows.Value).ToList();
                break;
            }
        }

        return all;
    }

    private static List<string> SplitInsertBatches(string sql)
    {
        var list = new List<string>();
        var start = 0;
        var depth = 0;
        var inStr = false;
        char q = '\0';
        for (var i = 0; i < sql.Length; i++)
        {
            var c = sql[i];
            if (inStr)
            {
                if (c == q)
                {
                    if (q == '\'' && i + 1 < sql.Length && sql[i + 1] == '\'') { i++; continue; }
                    inStr = false;
                }
                continue;
            }

            if (c is '\'' or '"')
            {
                inStr = true;
                q = c;
                continue;
            }

            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ';' && depth == 0)
            {
                list.Add(sql[start..i]);
                start = i + 1;
            }
        }

        list.Add(sql[start..]);
        return list;
    }
}

internal static class InsertFolderNaming
{
    public static bool TryParseTableFromFileName(
        string fileName,
        string fileNaming,
        string? defaultSchema,
        InsertSqlDialect dialect,
        out TableRef table)
    {
        table = default;
        var stem = Path.GetFileNameWithoutExtension(fileName);
        var patternStem = Path.GetFileNameWithoutExtension(fileNaming);
        if (string.Equals(patternStem, "{table}", StringComparison.OrdinalIgnoreCase))
        {
            table = new TableRef(ResolveDefaultSchema(defaultSchema, dialect), stem);
            return true;
        }

        if (string.Equals(patternStem, "{schema}.{table}", StringComparison.OrdinalIgnoreCase))
        {
            var dot = stem.IndexOf('.');
            if (dot <= 0 || dot == stem.Length - 1)
                return false;
            table = new TableRef(stem[..dot], stem[(dot + 1)..]);
            return true;
        }

        return false;
    }

    private static string ResolveDefaultSchema(string? defaultSchema, InsertSqlDialect dialect)
    {
        if (!string.IsNullOrWhiteSpace(defaultSchema))
            return defaultSchema!;
        return dialect switch
        {
            InsertSqlDialect.SqlServer => "dbo",
            InsertSqlDialect.PostgreSql => "public",
            InsertSqlDialect.MySql => throw new InvalidOperationException(
                "For MySQL INSERT folders set project source.defaultSchema (database name) or use {schema}.{table}.sql file naming."),
            _ => throw new ArgumentOutOfRangeException(nameof(dialect)),
        };
    }

    public static string FileNameForTable(TableRef table, string fileNaming)
    {
        var patternStem = Path.GetFileNameWithoutExtension(fileNaming);
        var ext = Path.GetExtension(fileNaming);
        if (string.IsNullOrEmpty(ext)) ext = ".sql";
        if (string.Equals(patternStem, "{table}", StringComparison.OrdinalIgnoreCase))
            return $"{table.Name}{ext}";
        if (string.Equals(patternStem, "{schema}.{table}", StringComparison.OrdinalIgnoreCase))
            return $"{table.Schema}.{table.Name}{ext}";
        throw new InvalidOperationException($"Unsupported file naming pattern: {fileNaming}");
    }
}
