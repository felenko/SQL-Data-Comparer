namespace SqlDataCompare.Sync;

/// <summary>Phase 2 hook: generate SQL to reconcile destination to match source.</summary>
public interface ISyncScriptGenerator
{
    /// <summary>Dry-run script generation only (no execution).</summary>
    Task<string> GenerateScriptAsync(CancellationToken cancellationToken = default);
}
