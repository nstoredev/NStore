using System;
using NStore.Domain.Poco;
using Xunit;

namespace NStore.Domain.Tests
{
    public class DelegateToMethodProcessorTests
    {
        private class TestState
        {
            public int ExecuteCallCount { get; private set; }
            public object LastCommand { get; private set; }

            public object Execute(object command)
            {
                ExecuteCallCount++;
                LastCommand = command;
                return $"Executed: {command}";
            }
        }

        private class TestStateWithCustomMethod
        {
            public bool WasCalled { get; private set; }

            public object Process(object command)
            {
                WasCalled = true;
                return command;
            }
        }

        private class TestStateWithoutExecuteMethod
        {
            public void SomeOtherMethod()
            {
            }
        }

        private class TestStateWithPrivateExecute
        {
            private object Execute(object command)
            {
                return command;
            }
        }

        [Fact]
        public void RunCommand_should_invoke_Execute_method_on_state()
        {
            var processor = new DelegateToMethodProcessor();
            var state = new TestState();
            var command = "test command";

            var result = processor.RunCommand(state, command);

            Assert.Equal(1, state.ExecuteCallCount);
            Assert.Equal("test command", state.LastCommand);
            Assert.Equal("Executed: test command", result);
        }

        [Fact]
        public void RunCommand_should_invoke_custom_method_name()
        {
            var processor = new DelegateToMethodProcessor("Process");
            var state = new TestStateWithCustomMethod();
            var command = "test";

            var result = processor.RunCommand(state, command);

            Assert.True(state.WasCalled);
            Assert.Equal("test", result);
        }

        [Fact]
        public void RunCommand_should_throw_when_method_not_found()
        {
            var processor = new DelegateToMethodProcessor();
            var state = new TestStateWithoutExecuteMethod();

            var ex = Assert.Throws<MissingMethodException>(() => processor.RunCommand(state, "cmd"));

            Assert.Contains("Execute", ex.Message);
            Assert.Contains(nameof(TestStateWithoutExecuteMethod), ex.Message);
        }

        [Fact]
        public void RunCommand_should_throw_when_method_is_private()
        {
            var processor = new DelegateToMethodProcessor();
            var state = new TestStateWithPrivateExecute();

            var ex = Assert.Throws<MissingMethodException>(() => processor.RunCommand(state, "cmd"));

            Assert.Contains("Execute", ex.Message);
        }

        [Fact]
        public void RunCommand_should_cache_method_lookup()
        {
            var processor = new DelegateToMethodProcessor();
            var state1 = new TestState();
            var state2 = new TestState();

            // Call multiple times with same state type
            processor.RunCommand(state1, "cmd1");
            processor.RunCommand(state1, "cmd2");
            processor.RunCommand(state2, "cmd3");

            Assert.Equal(2, state1.ExecuteCallCount);
            Assert.Equal(1, state2.ExecuteCallCount);
        }

        [Fact]
        public void Multiple_processors_should_share_cache()
        {
            var processor1 = new DelegateToMethodProcessor();
            var processor2 = new DelegateToMethodProcessor();
            var state = new TestState();

            processor1.RunCommand(state, "cmd1");
            processor2.RunCommand(state, "cmd2");

            // Both should work and share the cached method
            Assert.Equal(2, state.ExecuteCallCount);
        }

        [Fact]
        public void Default_constructor_should_use_Execute_method()
        {
            var processor = new DelegateToMethodProcessor();
            var state = new TestState();

            processor.RunCommand(state, "test");

            Assert.Equal(1, state.ExecuteCallCount);
        }
    }
}
