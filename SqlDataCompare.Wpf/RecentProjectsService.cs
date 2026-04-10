using System.IO;
using System.Text.Json;

namespace SqlDataCompare.Wpf;

public record RecentProjectEntry(string Path)
{
    public string DisplayName => System.IO.Path.GetFileName(Path);
}

/// <summary>Persists the most-recently-used project file list to %APPDATA%\SqlDataCompare\recentprojects.json.</summary>
internal static class RecentProjectsService
{
    private const int MaxEntries = 10;

    private static string StoragePath { get; } = System.IO.Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "SqlDataCompare",
        "recentprojects.json");

    public static List<string> Load()
    {
        try
        {
            if (!File.Exists(StoragePath))
                return [];
            var json = File.ReadAllText(StoragePath);
            var list = JsonSerializer.Deserialize<List<string>>(json);
            // Filter out paths that no longer exist
            return list?.Where(File.Exists).Take(MaxEntries).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    public static List<string> AddAndSave(IEnumerable<string> existing, string newPath)
    {
        var list = new List<string> { newPath };
        list.AddRange(existing.Where(p => !string.Equals(p, newPath, StringComparison.OrdinalIgnoreCase)));
        if (list.Count > MaxEntries)
            list = list.Take(MaxEntries).ToList();
        Save(list);
        return list;
    }

    private static void Save(List<string> paths)
    {
        try
        {
            var dir = System.IO.Path.GetDirectoryName(StoragePath)!;
            Directory.CreateDirectory(dir);
            File.WriteAllText(StoragePath, JsonSerializer.Serialize(paths, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch
        {
            // Non-fatal — recent list is best-effort
        }
    }
}
