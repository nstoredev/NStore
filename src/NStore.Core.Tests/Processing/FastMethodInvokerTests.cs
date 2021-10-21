using System;
using NStore.Core.Processing;
using Xunit;

namespace NStore.Core.Tests.Processing
{
    public class FastMethodInvokerTests
    {
        private readonly Target _target = new Target();

        [Fact]
        public void invoker_correctly_invoke_method()
        {
            //even if we know that the method we are invoking is an action (void return) wrapped
            //fast method invoker let you use the return value, that is se to null by default
            //this reduce the need for the callre to know if the called method returns
            //or not returns a value.
            var voidRet = FastMethodInvoker.CallDynamically(_target, "DoSomething", "hello");
            Assert.Equal("hello", _target.Param);
            Assert.Null(voidRet);
        }

        [Fact]
        public void invoker_correctly_invoke_private_method()
        {
            var voidRet = FastMethodInvoker.CallDynamically(_target, "DoSomethingPrivate", "hello");
            Assert.Equal("hello", _target.Param);
            Assert.Null(voidRet);
        }

        [Fact]
        public void invoker_do_not_have_problem_if_method_does_not_exists()
        {
            FastMethodInvoker.CallDynamically(_target, "Not_existing_method", "hello");
            Assert.Null(_target.Param);
        }

        [Fact]
        public void invoker_correctly_invoke_method_with_return_type()
        {
            var result = (string)FastMethodInvoker.CallDynamically(_target, "DoSomethingReturn", "hello");
            Assert.Equal("hello", _target.Param);
            Assert.Equal("processed hello", result);
        }

        [Fact]
        public void invoker_correctly_dispatch_to_object()
        {
            var result = (string)FastMethodInvoker.CallDynamically(_target, "DoSomethingWithObjectReturn", new object());
            Assert.Equal("processed", result);
        }

        [Fact]
        public void invoker_on_optional_public_method_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                FastMethodInvoker.CallDynamically(_target, "FailPublic", new object())
            );
        }

        [Fact]
        public void invoker_on_private_method_should_not_mask_exception()
        {
            Assert.Throws<TargetException>(() =>
                FastMethodInvoker.CallDynamically(_target, "FailPrivate", new object())
            );
        }

        //[Fact]
        //public void invoker_on_private_methods_should_not_mask_exception()
        //{
        //    Assert.Throws<TargetException>(() =>
        //        FastMethodInvoker.CallNonPublicIfExists(_target, new[] { "FailPrivate", "FailPrivate" }, new object())
        //    );
        //}
    }
}