using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqlDataCompare.Project;

public static class CompareProjectSerializer
{
    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public static CompareProject Read(string path)
    {
        var json = File.ReadAllText(path);
        return Parse(json);
    }

    /// <summary>Deserialize a compare project from JSON (same shape as a saved project file).</summary>
    public static CompareProject Parse(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        return JsonSerializer.Deserialize<CompareProject>(json, JsonOptions)
               ?? throw new InvalidOperationException("Project JSON was empty or invalid.");
    }

    public static void Write(string path, CompareProject project)
    {
        var json = JsonSerializer.Serialize(project, JsonOptions);
        File.WriteAllText(path, json);
    }
}
