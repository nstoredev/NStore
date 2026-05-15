using Moq;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using Xunit;

namespace NStore.Core.Tests.Persistence
{
    public class LogDecoratorTests
    {
        private readonly Mock<INStoreLogger> _loggerMock;
        private readonly IPersistence _store;

        public LogDecoratorTests()
        {
            _loggerMock = new Mock<INStoreLogger>();
            var loggerFactoryMock = new Mock<INStoreLoggerFactory>();
            loggerFactoryMock
                .Setup(x => x.CreateLogger(It.IsAny<string>()))
                .Returns(_loggerMock.Object);

            _store = new LogDecorator(new NullPersistence(), loggerFactoryMock.Object);
        }

        [Fact]
        public void sync_read_forward_should_be_logged()
        {
            _store.ReadForward("empty", 0, long.MaxValue, 10);

            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("Start ReadPartitionForward")),
                It.IsAny<object[]>()), Times.Once);
            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("End ReadPartitionForward")),
                It.IsAny<object[]>()), Times.Once);
        }

        [Fact]
        public void sync_read_backward_should_be_logged()
        {
            _store.ReadBackward("empty", long.MaxValue, 0, 10);

            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("Start ReadPartitionBackward")),
                It.IsAny<object[]>()), Times.Once);
            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("End ReadPartitionBackward")),
                It.IsAny<object[]>()), Times.Once);
        }

        [Fact]
        public void sync_read_single_backward_should_be_logged()
        {
            _store.ReadSingleBackward("empty", long.MaxValue);

            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("Start ReadLast")),
                It.IsAny<object[]>()), Times.Once);
            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("End ReadLast")),
                It.IsAny<object[]>()), Times.Once);
        }

        [Fact]
        public void sync_read_by_operation_id_should_be_logged()
        {
            _store.ReadByOperationId("empty", "operation");

            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("Start ReadByOperationId")),
                It.IsAny<object[]>()), Times.Once);
            _loggerMock.Verify(x => x.LogDebug(
                It.Is<string>(message => message.StartsWith("End ReadByOperationId")),
                It.IsAny<object[]>()), Times.Once);
        }
    }
}
