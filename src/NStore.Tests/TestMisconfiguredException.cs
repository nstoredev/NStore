using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Persistence.Tests
{
    public class TestMisconfiguredException : Exception
    {
        public TestMisconfiguredException(string message) : base(message)
        {
        }
    }
}
