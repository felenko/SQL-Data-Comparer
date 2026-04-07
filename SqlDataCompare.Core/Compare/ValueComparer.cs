using System.Globalization;

namespace SqlDataCompare.Compare;

public sealed class ValueComparer
{
    private readonly bool _trimStrings;
    private readonly StringComparison _stringComparison;

    public ValueComparer(bool ordinalIgnoreCase, bool trimStrings)
    {
        _trimStrings = trimStrings;
        _stringComparison = ordinalIgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
    }

    public bool Equal(object? a, object? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;

        if (a is string sa && b is string sb)
        {
            if (_trimStrings)
            {
                sa = sa.Trim();
                sb = sb.Trim();
            }

            return string.Equals(sa, sb, _stringComparison);
        }

        if (a is string sas && b is not string)
            return Equal(ConvertForCompare(sas, b), b);
        if (b is string sbs && a is not string)
            return Equal(a, ConvertForCompare(sbs, a));

        if (IsNumeric(a) && IsNumeric(b))
            return Convert.ToDecimal(a, CultureInfo.InvariantCulture) == Convert.ToDecimal(b, CultureInfo.InvariantCulture);

        if (a is DateTime da && b is DateTime db)
            return da.Equals(db);

        if (a is DateTimeOffset dxa && b is DateTimeOffset dxb)
            return dxa.Equals(dxb);

        if (a is bool ba && b is bool bb)
            return ba == bb;

        return Equals(a, b) || string.Equals(a.ToString(), b.ToString(), _stringComparison);
    }

    private static bool IsNumeric(object o) => o is byte or short or int or long or float or double or decimal;

    private static object? ConvertForCompare(string s, object other)
    {
        if (other is long or int or short or byte)
        {
            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
                return l;
        }

        if (other is decimal)
        {
            if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var d))
                return d;
        }

        if (other is double or float)
        {
            if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                return d;
        }

        return s;
    }
}
