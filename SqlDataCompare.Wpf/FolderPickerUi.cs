using System.IO;
using System.Windows;
using System.Windows.Forms;
using System.Windows.Interop;

namespace SqlDataCompare.Wpf;

public static class FolderPickerUi
{
    public static bool TryPickFolder(Window? owner, string? initialPath, out string folderPath)
    {
        folderPath = "";
        using var dlg = new FolderBrowserDialog
        {
            ShowNewFolderButton = true,
            UseDescriptionForTitle = true,
            Description = "Select folder containing .sql INSERT scripts.",
        };
        if (!string.IsNullOrWhiteSpace(initialPath))
        {
            try
            {
                var p = Path.GetFullPath(initialPath.Trim());
                dlg.InitialDirectory = Directory.Exists(p) ? p : Path.GetDirectoryName(p) ?? p;
            }
            catch
            {
                /* ignore */
            }
        }

        var handle = owner is null ? IntPtr.Zero : new WindowInteropHelper(owner).Handle;
        var result = handle == IntPtr.Zero
            ? dlg.ShowDialog()
            : dlg.ShowDialog(new Win32WindowOwner(handle));
        if (result != DialogResult.OK || string.IsNullOrWhiteSpace(dlg.SelectedPath))
            return false;
        folderPath = dlg.SelectedPath;
        return true;
    }
}
