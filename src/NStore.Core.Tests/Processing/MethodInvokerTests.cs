using System;
using NStore.Core.Processing;
using Xunit;

namespace NStore.Core.Tests.Processing
{
    public class MethodInvokerTests
    {
        private readonly Target _target = new Target();

        [Fact]
        public void invoker_on_public_method_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                _target.CallPublic("FailPublic", new object())
            );
        }

        [Fact]
        public void invoker_on_optional_public_method_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                _target.CallPublicIfExists("FailPublic", new object())
            );
        }

        [Fact]
        public void invoker_on_private_method_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                _target.CallNonPublicIfExists("FailPrivate", new object())
            );
        }

        [Fact]
        public void invoker_on_private_methods_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                _target.CallNonPublicIfExists(new[] {"FailPrivate", "FailPrivate"}, new object())
            );
        }
    }
}