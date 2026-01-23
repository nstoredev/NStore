using System;
using System.Collections.Generic;
using System.Linq;

namespace NStore.Domain
{
    public class BatchSaveResult
    {
        public static BatchSaveResult Empty => new BatchSaveResult();

        public IReadOnlyList<AggregateSaveResult> Results { get; set; } = Array.Empty<AggregateSaveResult>();

        public bool HasFailures => Results != null && Results.Any(r => !r.Succeeded);

        public bool Success => !HasFailures;
    }
}