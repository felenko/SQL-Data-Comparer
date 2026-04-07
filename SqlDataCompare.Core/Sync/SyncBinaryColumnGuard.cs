using SqlDataCompare.Schema;

namespace SqlDataCompare.Sync;

/// <summary>Avoids binding CLR <see cref="string"/> to SQL Server <c>image</c>/<c>varbinary</c> (error 402 type clash).</summary>
internal static class SyncBinaryColumnGuard
{
    internal static bool ShouldOmitValue(string? destPhysicalType, object? value)
    {
        if (!BinaryColumnHeuristics.IsBinaryLikeType(destPhysicalType))
            return false;
        if (value is null || value is DBNull)
            return false;
        return value is string;
    }
}
