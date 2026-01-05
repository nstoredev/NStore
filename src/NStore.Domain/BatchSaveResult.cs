using System.Collections.Generic;
using System.Linq;

namespace NStore.Domain
{
    public class BatchSaveResult
    {
        public IReadOnlyList<AggregateSaveResult> Results { get; set; } = new List<AggregateSaveResult>();

        public bool HasFailures => Results != null && Results.Any(r => !r.Succeeded);
        public bool Success => !HasFailures;
    }
}