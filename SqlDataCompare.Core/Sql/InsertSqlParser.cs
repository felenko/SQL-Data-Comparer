using System.Globalization;
using System.Text;

namespace SqlDataCompare.Sql;

/// <summary>Parses a subset of INSERT … VALUES statements into rows. Multiple tuples after VALUES supported.</summary>
public static class InsertSqlParser
{
    public sealed class Result
    {
        public List<string>? ExplicitColumnNames { get; init; }
        public required List<Dictionary<string, object?>> Rows { get; init; }
    }

    public static Result Parse(string sql, InsertSqlDialect dialect, IReadOnlyList<string>? fallbackColumnNames)
    {
        var s = sql.AsSpan();
        var i = SkipWs(s, 0);
        i = ExpectInsensitive(s, i, "INSERT");
        i = SkipWs(s, i);
        i = ExpectInsensitive(s, i, "INTO");
        i = SkipWs(s, i);
        i = SkipQualifiedTableName(s, i, dialect);
        i = SkipWs(s, i);
        List<string>? colNames = null;
        if (s.Length > i && s[i] == '(')
        {
            var endParen = FindMatchingParen(s, i, dialect);
            colNames = SplitColumnList(s.Slice(i + 1, endParen - i - 1), dialect);
            i = endParen + 1;
            i = SkipWs(s, i);
        }

        i = ExpectInsensitive(s, i, "VALUES");
        i = SkipWs(s, i);
        var rows = new List<Dictionary<string, object?>>();
        while (i < s.Length)
        {
            if (s[i] != '(')
                throw new InvalidOperationException("Expected '(' starting VALUES tuple.");
            var tupleEnd = FindMatchingParen(s, i, dialect);
            var tuple = s.Slice(i + 1, tupleEnd - i - 1);
            var values = SplitTopLevelCommaList(tuple, dialect);
            var names = colNames ?? fallbackColumnNames ?? GenerateNames(values.Count);
            if (names.Count != values.Count)
                throw new InvalidOperationException($"Column count ({names.Count}) does not match VALUES count ({values.Count}).");
            var row = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (var c = 0; c < names.Count; c++)
                row[names[c]] = ParseLiteral(values[c].AsSpan(), dialect);
            rows.Add(row);
            i = tupleEnd + 1;
            i = SkipWs(s, i);
            if (i < s.Length && s[i] == ',')
            {
                i++;
                i = SkipWs(s, i);
                continue;
            }
            break;
        }

        return new Result { ExplicitColumnNames = colNames, Rows = rows };
    }

    private static List<string> GenerateNames(int n)
    {
        var list = new List<string>(n);
        for (var i = 0; i < n; i++)
            list.Add($"Col{i + 1}");
        return list;
    }

    private static int SkipQualifiedTableName(ReadOnlySpan<char> s, int start, InsertSqlDialect dialect)
    {
        var i = start;
        if (i >= s.Length)
            throw new InvalidOperationException("Expected table name.");
        if (dialect == InsertSqlDialect.SqlServer && s[i] == '[')
        {
            i = FindClosingBracket(s, i) + 1;
            if (i < s.Length && s[i] == '.')
            {
                i++;
                if (i >= s.Length || s[i] != '[')
                    throw new InvalidOperationException("Expected bracketed identifier after dot.");
                i = FindClosingBracket(s, i) + 1;
            }
            return i;
        }

        while (i < s.Length && !char.IsWhiteSpace(s[i]) && s[i] != '(')
        {
            var c = s[i];
            if (c == '`' || c == '"')
            {
                i++;
                while (i < s.Length && s[i] != c) i++;
                if (i >= s.Length)
                    throw new InvalidOperationException("Unclosed quoted identifier.");
                i++;
                if (i < s.Length && s[i] == '.')
                {
                    i++;
                    continue;
                }
                break;
            }

            if (c == '.')
            {
                i++;
                continue;
            }

            i++;
        }

        return i;
    }

    private static int FindClosingBracket(ReadOnlySpan<char> s, int openIdx)
    {
        var depth = 0;
        for (var p = openIdx; p < s.Length; p++)
        {
            if (s[p] == '[') depth++;
            else if (s[p] == ']')
            {
                depth--;
                if (depth == 0) return p;
            }
        }

        throw new InvalidOperationException("Unclosed bracket identifier.");
    }

    private static int ExpectInsensitive(ReadOnlySpan<char> s, int i, string word)
    {
        if (i + word.Length > s.Length)
            throw new InvalidOperationException($"Expected {word}.");
        for (var w = 0; w < word.Length; w++)
        {
            if (char.ToUpperInvariant(s[i + w]) != char.ToUpperInvariant(word[w]))
                throw new InvalidOperationException($"Expected {word}.");
        }

        return i + word.Length;
    }

    private static int SkipWs(ReadOnlySpan<char> s, int i)
    {
        while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
        return i;
    }

    private static int FindMatchingParen(ReadOnlySpan<char> s, int openIdx, InsertSqlDialect dialect)
    {
        var depth = 0;
        var inStr = false;
        char strQuote = '\0';
        for (var p = openIdx; p < s.Length; p++)
        {
            var c = s[p];
            if (inStr)
            {
                if (c == strQuote)
                {
                    if (strQuote == '\'' && dialect == InsertSqlDialect.SqlServer && p + 1 < s.Length && s[p + 1] == '\'')
                    {
                        p++;
                        continue;
                    }

                    if (p + 1 < s.Length && s[p + 1] == strQuote && strQuote is '"' or '`')
                    {
                        p++;
                        continue;
                    }

                    inStr = false;
                }
                continue;
            }

            if (dialect == InsertSqlDialect.SqlServer && c == 'N' && p + 1 < s.Length && s[p + 1] == '\'')
            {
                inStr = true;
                strQuote = '\'';
                p++;
                continue;
            }

            if (c is '\'' or '"')
            {
                inStr = true;
                strQuote = c;
                continue;
            }

            if (c == '(') depth++;
            else if (c == ')')
            {
                depth--;
                if (depth == 0) return p;
            }
        }

        throw new InvalidOperationException("Unclosed parenthesis.");
    }

    private static List<string> SplitColumnList(ReadOnlySpan<char> inner, InsertSqlDialect dialect)
    {
        var parts = SplitTopLevelCommaList(inner, dialect);
        var cols = new List<string>(parts.Count);
        foreach (var p in parts)
            cols.Add(NormalizeIdentifier(p.AsSpan(), dialect).ToString());
        return cols;
    }

    private static ReadOnlySpan<char> NormalizeIdentifier(ReadOnlySpan<char> p, InsertSqlDialect dialect)
    {
        p = p.Trim();
        if (p.Length == 0) return p;
        if (dialect == InsertSqlDialect.SqlServer && p[0] == '[')
        {
            var end = FindClosingBracket(p.ToString().AsSpan(), 0);
            p = p.Slice(1, end - 1);
            return UnescapeSqlServerBracket(p);
        }

        if ((dialect == InsertSqlDialect.PostgreSql || dialect == InsertSqlDialect.MySql) && (p[0] == '"' || p[0] == '`'))
        {
            var q = p[0];
            if (p[^1] != q)
                throw new InvalidOperationException("Mismatched quoted identifier.");
            return p.Slice(1, p.Length - 2);
        }

        return p;
    }

    private static ReadOnlySpan<char> UnescapeSqlServerBracket(ReadOnlySpan<char> p)
    {
        var sb = new StringBuilder(p.Length);
        for (var i = 0; i < p.Length; i++)
        {
            if (p[i] == ']' && i + 1 < p.Length && p[i + 1] == ']')
            {
                sb.Append(']');
                i++;
            }
            else sb.Append(p[i]);
        }

        return sb.ToString().AsSpan();
    }

    private static List<string> SplitTopLevelCommaList(ReadOnlySpan<char> s, InsertSqlDialect dialect)
    {
        var list = new List<string>();
        var start = 0;
        var depthParen = 0;
        var depthBracket = 0;
        var inStr = false;
        char strQ = '\0';
        for (var i = 0; i <= s.Length; i++)
        {
            if (i == s.Length || (!inStr && depthParen == 0 && depthBracket == 0 && s[i] == ','))
            {
                var slice = s.Slice(start, i - start).Trim();
                if (slice.Length > 0)
                    list.Add(slice.ToString());
                start = i + 1;
                continue;
            }

            if (i >= s.Length) break;
            var c = s[i];
            if (inStr)
            {
                if (c == strQ)
                {
                    if (dialect == InsertSqlDialect.SqlServer && strQ == '\'' && i + 1 < s.Length && s[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    if (i + 1 < s.Length && s[i + 1] == strQ && strQ is '"' or '`')
                    {
                        i++;
                        continue;
                    }

                    inStr = false;
                }
                continue;
            }

            if (c is '\'' or '"')
            {
                inStr = true;
                strQ = c;
                continue;
            }

            if (dialect == InsertSqlDialect.SqlServer && c == 'N' && i + 1 < s.Length && s[i + 1] == '\'')
            {
                i++;
                inStr = true;
                strQ = '\'';
                continue;
            }

            if (dialect == InsertSqlDialect.MySql && c == '`')
            {
                inStr = true;
                strQ = '`';
                continue;
            }

            if (c == '(') depthParen++;
            else if (c == ')') depthParen--;
            else if (dialect == InsertSqlDialect.SqlServer && c == '[') depthBracket++;
            else if (dialect == InsertSqlDialect.SqlServer && c == ']') depthBracket--;
        }

        return list;
    }

    private static object? ParseLiteral(ReadOnlySpan<char> lit, InsertSqlDialect dialect)
    {
        lit = lit.Trim();
        if (lit.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;
        if (lit.Length >= 2 && lit[0] == '\'' && lit[^1] == '\'')
            return UnquoteSqlString(lit.Slice(1, lit.Length - 2), dialect, '\'');
        if (dialect == InsertSqlDialect.SqlServer && lit.Length >= 3 && char.ToUpperInvariant(lit[0]) == 'N' && lit[1] == '\'' && lit[^1] == '\'')
            return UnquoteSqlString(lit.Slice(2, lit.Length - 3), dialect, '\'');
        if (lit.Length >= 2 && lit[0] == '"' && lit[^1] == '"')
            return UnquoteSqlString(lit.Slice(1, lit.Length - 2), dialect, '"');
        if (dialect == InsertSqlDialect.MySql && lit.Length >= 2 && lit[0] == '`' && lit[^1] == '`')
            return lit.Slice(1, lit.Length - 2).ToString();

        if (lit.Equals("TRUE", StringComparison.OrdinalIgnoreCase)) return true;
        if (lit.Equals("FALSE", StringComparison.OrdinalIgnoreCase)) return false;

        if (long.TryParse(lit, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
            return l;
        if (decimal.TryParse(lit, NumberStyles.Number, CultureInfo.InvariantCulture, out var d))
            return d;
        if (double.TryParse(lit, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var dbl))
            return dbl;
        return lit.ToString();
    }

    private static string UnquoteSqlString(ReadOnlySpan<char> inner, InsertSqlDialect dialect, char q)
    {
        var sb = new StringBuilder(inner.Length);
        for (var i = 0; i < inner.Length; i++)
        {
            if (inner[i] == q && i + 1 < inner.Length && inner[i + 1] == q)
            {
                sb.Append(q);
                i++;
            }
            else sb.Append(inner[i]);
        }

        _ = dialect;
        return sb.ToString();
    }
}
