using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace SqlDataCompare.Wpf.Converters;

/// <summary>True → Collapsed, False → Visible (e.g. hide “Compare” while a run is active).</summary>
[ValueConversion(typeof(bool), typeof(Visibility))]
public sealed class InverseBooleanToVisibilityConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is true ? Visibility.Collapsed : Visibility.Visible;

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is Visibility v && v != Visibility.Visible;
}
