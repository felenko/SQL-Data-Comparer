using System.Globalization;
using System.Windows.Data;

namespace SqlDataCompare.Wpf.Converters;

/// <summary>Inverts a boolean (e.g. enable controls when not busy).</summary>
[ValueConversion(typeof(bool), typeof(bool))]
public sealed class InvertedBooleanConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is bool b ? !b : true;

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is bool b ? !b : false;
}
