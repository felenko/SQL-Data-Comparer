using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Runtime.Versioning;

namespace SqlDataCompare.Connection;

/// <summary>Windows-only protection for saved connection strings.</summary>
[SupportedOSPlatform("windows")]
public static class DpapiConnectionString
{
    public static bool IsSupported => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    public static string ProtectToBase64(string plaintext)
    {
        if (!IsSupported)
            throw new PlatformNotSupportedException("DPAPI is only available on Windows.");
        var bytes = System.Text.Encoding.UTF8.GetBytes(plaintext);
        var protectedBytes = ProtectedData.Protect(bytes, null, DataProtectionScope.CurrentUser);
        return Convert.ToBase64String(protectedBytes);
    }

    public static string UnprotectFromBase64(string base64)
    {
        if (!IsSupported)
            throw new PlatformNotSupportedException("DPAPI is only available on Windows.");
        var protectedBytes = Convert.FromBase64String(base64);
        var bytes = ProtectedData.Unprotect(protectedBytes, null, DataProtectionScope.CurrentUser);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }
}
