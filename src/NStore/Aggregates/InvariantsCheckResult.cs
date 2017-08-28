namespace NStore.Aggregates
{
    public sealed class InvariantsCheckResult
    {
        public static InvariantsCheckResult Ok = new InvariantsCheckResult();

        public string Message { get; private set; }
        public bool IsInvalid => !IsValid;
        public bool IsValid { get; private set; }

        private InvariantsCheckResult()
        {
            this.IsValid = true;
        }

        public static InvariantsCheckResult Invalid(string message)
        {
            return new InvariantsCheckResult()
            {
                Message = message,
                IsValid = false
            };
        }

        public void ThrowIfInvalid()
        {
            if (IsValid)
                return;

            throw new InvariantCheckFailedException(this.Message);
        }
    }
}