using System.Text.RegularExpressions;

namespace SqlDataCompare.Connection;

public static partial class ConnectionStringExpander
{
    [GeneratedRegex(@"\$\{ENV:([^}]+)\}", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex EnvVarRegex();

    public static string Expand(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return value ?? string.Empty;
        return EnvVarRegex().Replace(value, m =>
        {
            var name = m.Groups[1].Value.Trim();
            return Environment.GetEnvironmentVariable(name) ?? string.Empty;
        });
    }
}
