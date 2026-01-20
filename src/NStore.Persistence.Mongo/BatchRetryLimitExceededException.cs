using System;

namespace NStore.Persistence.Mongo
{
    /// <summary>
    /// Exception thrown when the batch append operation exceeds the maximum number of retry attempts.
    /// This typically indicates a systemic issue such as sequence generator failure or extreme contention.
    /// </summary>
    public class BatchRetryLimitExceededException : MongoPersistenceException
    {
        public int RetryCount { get; }
        public int MaxRetries { get; }
        public int FailedJobsCount { get; }

        public BatchRetryLimitExceededException(int retryCount, int maxRetries, int failedJobsCount)
            : base($"Batch append operation exceeded maximum retry limit. Retries: {retryCount}/{maxRetries}, Failed jobs: {failedJobsCount}. " +
                   "This indicates a systemic problem such as sequence generator failure or extreme contention.")
        {
            RetryCount = retryCount;
            MaxRetries = maxRetries;
            FailedJobsCount = failedJobsCount;
        }

        public BatchRetryLimitExceededException(int retryCount, int maxRetries, int failedJobsCount, Exception innerException)
            : base($"Batch append operation exceeded maximum retry limit. Retries: {retryCount}/{maxRetries}, Failed jobs: {failedJobsCount}. " +
                   "This indicates a systemic problem such as sequence generator failure or extreme contention.", innerException)
        {
            RetryCount = retryCount;
            MaxRetries = maxRetries;
            FailedJobsCount = failedJobsCount;
        }
    }
}
