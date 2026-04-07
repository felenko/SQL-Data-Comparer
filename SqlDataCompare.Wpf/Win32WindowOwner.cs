using System.Windows.Forms;

namespace SqlDataCompare.Wpf;

/// <summary>Bridges a HWND to <see cref="IWin32Window"/> for native/WinForms dialogs owned by WPF.</summary>
internal sealed class Win32WindowOwner(IntPtr handle) : IWin32Window
{
    public IntPtr Handle { get; } = handle;
}
