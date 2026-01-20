using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NStore.Core.Logging;
using NStore.Core.Persistence;
using NStore.Tpl;
using Xunit;

namespace NStore.Core.Tests.Tpl
{
    public class PersistenceBatchAppendDecoratorTests : IDisposable
    {
        private readonly TestEnhancedPersistence _persistence;
        private readonly Mock<INStoreLogger> _loggerMock;
        private PersistenceBatchAppendDecorator _decorator;

        public PersistenceBatchAppendDecoratorTests()
        {
            _persistence = new TestEnhancedPersistence();
            _loggerMock = new Mock<INStoreLogger>();
        }

        public void Dispose()
        {
            _decorator?.Dispose();
        }

        [Fact]
        public async Task AppendAsync_SingleWrite_ShouldSucceed()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);

            // Act
            var chunk = await _decorator.AppendAsync("partition1", 1, "payload1", "op1", CancellationToken.None);

            // Assert
            Assert.NotNull(chunk);
            Assert.Equal("partition1", chunk.PartitionId);
            Assert.Equal(1, chunk.Index);
            Assert.Equal("payload1", chunk.Payload);
            Assert.Equal("op1", chunk.OperationId);
        }

        [Fact]
        public async Task AppendAsync_MultipleWrites_ShouldBatchAndSucceed()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 5, flushTimeout: 100);

            // Act
            var tasks = new List<Task<IChunk>>();
            for (int i = 1; i <= 10; i++)
            {
                tasks.Add(_decorator.AppendAsync($"partition{i % 3}", i, $"payload{i}", $"op{i}", CancellationToken.None));
            }

            var chunks = await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(10, chunks.Length);
            Assert.All(chunks, chunk => Assert.NotNull(chunk));
            
            // Verify all chunks were written
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(10, lastPosition);
        }

        [Fact]
        public async Task AppendAsync_TriggersBatchBySize_ShouldFlushWhenBatchSizeReached()
        {
            // Arrange
            var batchSize = 3;
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: batchSize, flushTimeout: 5000);

            // Act
            var tasks = new List<Task<IChunk>>();
            for (int i = 1; i <= batchSize; i++)
            {
                tasks.Add(_decorator.AppendAsync("partition1", i, $"payload{i}", null, CancellationToken.None));
            }

            var chunks = await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(batchSize, chunks.Length);
            Assert.All(chunks, chunk => Assert.NotNull(chunk));
        }

        [Fact]
        public async Task AppendAsync_TriggersBatchByTimeout_ShouldFlushAfterTimeout()
        {
            // Arrange
            var flushTimeout = 50; // 50ms
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 100, flushTimeout: flushTimeout);

            // Act
            var chunk = await _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Give time for timeout-based flush
            await Task.Delay(flushTimeout * 3);

            // Assert
            Assert.NotNull(chunk);
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(1, lastPosition);
        }

        [Fact]
        public async Task AppendAsync_DuplicateIndex_ShouldReturnNull()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);

            // Act
            var chunk1 = await _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            var chunk2 = await _decorator.AppendAsync("partition1", 1, "payload2", null, CancellationToken.None);

            // Assert
            Assert.NotNull(chunk1);
            Assert.Null(chunk2); // Duplicate index should return null
        }

        [Fact]
        public async Task AppendAsync_DuplicateOperationId_ShouldReturnNull()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);

            // Act
            var chunk1 = await _decorator.AppendAsync("partition1", 1, "payload1", "op1", CancellationToken.None);
            var chunk2 = await _decorator.AppendAsync("partition1", 2, "payload2", "op1", CancellationToken.None);

            // Assert
            Assert.NotNull(chunk1);
            Assert.Null(chunk2); // Duplicate operation ID should return null
        }

        [Fact]
        public async Task AppendAsync_ConcurrentWrites_ShouldHandleCorrectly()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            var writeCount = 50;

            // Act
            var tasks = Enumerable.Range(1, writeCount)
                .Select(i => _decorator.AppendAsync($"partition{i % 5}", i, $"payload{i}", $"op{i}", CancellationToken.None))
                .ToList();

            var chunks = await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(writeCount, chunks.Length);
            Assert.All(chunks, chunk => Assert.NotNull(chunk));
            
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(writeCount, lastPosition);
        }

        [Fact]
        public async Task ShutdownAsync_ShouldFlushPendingWrites()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 100, flushTimeout: 10000);

            // Act
            var writeTask = _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _decorator.ShutdownAsync(CancellationToken.None);
            var chunk = await writeTask;

            // Assert
            Assert.NotNull(chunk);
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(1, lastPosition);
        }

        [Fact]
        public async Task Dispose_ShouldFlushPendingWrites()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 100, flushTimeout: 10000);

            // Act
            var writeTask = _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            _decorator.Dispose();
            var chunk = await writeTask;

            // Assert
            Assert.NotNull(chunk);
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(1, lastPosition);
        }

        [Fact]
        public async Task DisposeAsync_ShouldFlushPendingWrites()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 100, flushTimeout: 10000);

            // Act
            var writeTask = _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _decorator.DisposeAsync();
            var chunk = await writeTask;

            // Assert
            Assert.NotNull(chunk);
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(1, lastPosition);
        }

        [Fact]
        public async Task ReadForwardAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition1", 2, "payload2", null, CancellationToken.None);

            var recorder = new Recorder();

            // Act
            await _decorator.ReadForwardAsync("partition1", 0, recorder, long.MaxValue, int.MaxValue, CancellationToken.None);

            // Assert
            Assert.Equal(2, recorder.Length);
        }

        [Fact]
        public async Task ReadBackwardAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition1", 2, "payload2", null, CancellationToken.None);

            var recorder = new Recorder();

            // Act
            await _decorator.ReadBackwardAsync("partition1", long.MaxValue, recorder, 0, int.MaxValue, CancellationToken.None);

            // Assert
            Assert.Equal(2, recorder.Length);
            Assert.Equal(2L, recorder[0].Index);
            Assert.Equal(1L, recorder[1].Index);
        }

        [Fact]
        public async Task ReadSingleBackwardAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Act
            var chunk = await _decorator.ReadSingleBackwardAsync("partition1", 1, CancellationToken.None);

            // Assert
            Assert.NotNull(chunk);
            Assert.Equal("payload1", chunk.Payload);
        }

        [Fact]
        public async Task ReadAllAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition2", 1, "payload2", null, CancellationToken.None);

            var recorder = new AllPartitionsRecorder();

            // Act
            await _decorator.ReadAllAsync(0, recorder, int.MaxValue, CancellationToken.None);

            // Assert
            Assert.Equal(2, recorder.Length);
        }

        [Fact]
        public async Task ReadLastPositionAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Act
            var position = await _decorator.ReadLastPositionAsync(CancellationToken.None);

            // Assert
            Assert.Equal(1, position);
        }

        [Fact]
        public async Task DeleteAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition1", 2, "payload2", null, CancellationToken.None);

            // Act
            await _decorator.DeleteAsync("partition1", 0, long.MaxValue, CancellationToken.None);

            // Assert
            var recorder = new Recorder();
            await _decorator.ReadForwardAsync("partition1", 0, recorder, long.MaxValue, int.MaxValue, CancellationToken.None);
            Assert.Equal(0, recorder.Length);
        }

        [Fact]
        public async Task ReplaceOneAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            var original = await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Act
            var replaced = await _decorator.ReplaceOneAsync(original.Position, "partition1", 1, "newPayload", null, CancellationToken.None);

            // Assert
            Assert.NotNull(replaced);
            Assert.Equal("newPayload", replaced.Payload);
        }

        [Fact]
        public async Task ReadOneAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            var chunk = await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Act
            var read = await _decorator.ReadOneAsync(chunk.Position, CancellationToken.None);

            // Assert
            Assert.NotNull(read);
            Assert.Equal("payload1", read.Payload);
        }

        [Fact]
        public async Task ReadByOperationIdAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", "op1", CancellationToken.None);

            // Act
            var chunk = await _decorator.ReadByOperationIdAsync("partition1", "op1", CancellationToken.None);

            // Assert
            Assert.NotNull(chunk);
            Assert.Equal("payload1", chunk.Payload);
        }

        [Fact]
        public async Task ReadAllByOperationIdAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", "op1", CancellationToken.None);
            await _persistence.AppendAsync("partition2", 1, "payload2", "op1", CancellationToken.None);

            var recorder = new Recorder();

            // Act
            await _decorator.ReadAllByOperationIdAsync("op1", recorder, CancellationToken.None);

            // Assert
            Assert.Equal(2, recorder.Length);
        }

        [Fact]
        public void SupportsFillers_ShouldReturnUnderlyingPersistenceValue()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);

            // Act
            var supportsFillers = _decorator.SupportsFillers;

            // Assert
            Assert.Equal(_persistence.SupportsFillers, supportsFillers);
        }

        [Fact]
        public async Task AppendAsync_WithCancellationToken_ShouldRespectCancellation()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 100, flushTimeout: 10000);
            var cts = new CancellationTokenSource();
            cts.Cancel();

            // Act & Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await _decorator.AppendAsync("partition1", 1, "payload1", null, cts.Token));
        }

        [Fact]
        public async Task Dispose_WithTimeout_ShouldLogWarning()
        {
            // Arrange
            var mockPersistence = new Mock<IPersistence>();
            mockPersistence.As<IEnhancedPersistence>();
            
            // Make AppendBatchAsync hang indefinitely
            mockPersistence.As<IEnhancedPersistence>()
                .Setup(p => p.AppendBatchAsync(It.IsAny<WriteJob[]>(), It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await Task.Delay(Timeout.Infinite);
                });

            _decorator = new PersistenceBatchAppendDecorator(mockPersistence.Object, _loggerMock.Object, batchSize: 1, flushTimeout: 100);

            // Start a write operation that will trigger batch processing
            var writeTask = _decorator.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);

            // Give time for the batch to be triggered
            await Task.Delay(150);

            // Act
            _decorator.Dispose();

            // Assert
            _loggerMock.Verify(
                logger => logger.LogWarning(It.Is<string>(s => s.Contains("disposal timed out"))),
                Times.Once);
        }

        [Fact]
        public async Task BatchProcessing_UnderLoad_ShouldMaintainPerformance()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 50, flushTimeout: 50);
            var writeCount = 1000;

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            var tasks = Enumerable.Range(1, writeCount)
                .Select(i => _decorator.AppendAsync($"partition{i % 10}", i, $"payload{i}", $"op{i}", CancellationToken.None))
                .ToList();

            var chunks = await Task.WhenAll(tasks);
            stopwatch.Stop();

            // Assert
            Assert.Equal(writeCount, chunks.Length);
            Assert.All(chunks, chunk => Assert.NotNull(chunk));
            
            var lastPosition = await _persistence.ReadLastPositionAsync(CancellationToken.None);
            Assert.Equal(writeCount, lastPosition);
            
            // Performance assertion - should complete within reasonable time
            Assert.True(stopwatch.ElapsedMilliseconds < 5000, $"Batch processing took {stopwatch.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task ReadForwardMultiplePartitionsAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition2", 1, "payload2", null, CancellationToken.None);
            await _persistence.AppendAsync("partition3", 1, "payload3", null, CancellationToken.None);

            var recorder = new Recorder();
            var partitions = new[] { "partition1", "partition2" };

            // Act
            await _decorator.ReadForwardMultiplePartitionsAsync(partitions, 0, recorder, long.MaxValue, CancellationToken.None);

            // Assert
            Assert.Equal(2, recorder.Length);
        }

        [Fact]
        public async Task ReadForwardMultiplePartitionsWithRangesAsync_ShouldDelegateToUnderlyingPersistence()
        {
            // Arrange
            _decorator = new PersistenceBatchAppendDecorator(_persistence, _loggerMock.Object, batchSize: 10, flushTimeout: 100);
            await _persistence.AppendAsync("partition1", 1, "payload1", null, CancellationToken.None);
            await _persistence.AppendAsync("partition1", 2, "payload2", null, CancellationToken.None);
            await _persistence.AppendAsync("partition2", 1, "payload3", null, CancellationToken.None);
            await _persistence.AppendAsync("partition2", 2, "payload4", null, CancellationToken.None);
            await _persistence.AppendAsync("partition3", 1, "payload5", null, CancellationToken.None);

            var recorder = new Recorder();
            var requests = new[]
            {
                new PartitionReadRequest("partition1", 1, 1), // Only index 1 from partition1
                new PartitionReadRequest("partition2", 0, 2)  // indices 0-2 from partition2
            };

            // Act
            await _decorator.ReadForwardMultiplePartitionsWithRangesAsync(requests, recorder, CancellationToken.None);

            // Assert
            Assert.Equal(3, recorder.Length); // 1 from partition1 + 2 from partition2
            Assert.Contains(recorder.Chunks, c => c.PartitionId == "partition1" && c.Index == 1L);
            Assert.Contains(recorder.Chunks, c => c.PartitionId == "partition2" && c.Index == 1L);
            Assert.Contains(recorder.Chunks, c => c.PartitionId == "partition2" && c.Index == 2L);
        }

        // Helper class that implements both IPersistence and IEnhancedPersistence for testing
        private class TestEnhancedPersistence : IPersistence, IEnhancedPersistence
        {
            private readonly List<TestChunk> _chunks = new List<TestChunk>();
            private long _position = 0;
            private readonly object _lock = new object();

            public bool SupportsFillers => true;

            public Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    // Check for duplicate index
                    if (_chunks.Any(c => c.PartitionId == partitionId && c.Index == index))
                    {
                        throw new DuplicateStreamIndexException(partitionId, index);
                    }

                    // Check for duplicate operation ID
                    if (operationId != null && _chunks.Any(c => c.PartitionId == partitionId && c.OperationId == operationId))
                    {
                        throw new DuplicateStreamIndexException(partitionId, index);
                    }

                    _position++;
                    var chunk = new TestChunk
                    {
                        Position = _position,
                        PartitionId = partitionId,
                        Index = index,
                        Payload = payload,
                        OperationId = operationId
                    };
                    _chunks.Add(chunk);
                    return Task.FromResult<IChunk>(chunk);
                }
            }

            public async Task AppendBatchAsync(WriteJob[] jobs, CancellationToken cancellationToken)
            {
                foreach (var job in jobs)
                {
                    try
                    {
                        var chunk = await AppendAsync(job.PartitionId, job.Index, job.Payload, job.OperationId, cancellationToken);
                        job.Succeeded(chunk);
                    }
                    catch (DuplicateStreamIndexException)
                    {
                        if (_chunks.Any(c => c.PartitionId == job.PartitionId && c.Index == job.Index))
                        {
                            job.Failed(WriteJob.WriteResult.DuplicatedIndex);
                        }
                        else if (job.OperationId != null && _chunks.Any(c => c.PartitionId == job.PartitionId && c.OperationId == job.OperationId))
                        {
                            job.Failed(WriteJob.WriteResult.DuplicatedOperation);
                        }
                    }
                }
            }

            public Task ReadForwardAsync(string partitionId, long fromLowerIndexInclusive, ISubscription subscription, long toUpperIndexInclusive, int limit, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = _chunks
                        .Where(c => c.PartitionId == partitionId && c.Index >= fromLowerIndexInclusive && c.Index <= toUpperIndexInclusive)
                        .OrderBy(c => c.Index)
                        .Take(limit);

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

            public Task ReadForwardMultiplePartitionsAsync(IEnumerable<string> partitionIdsList, long fromLowerIndexInclusive, ISubscription subscription, long toUpperIndexInclusive, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = _chunks
                        .Where(c => partitionIdsList.Contains(c.PartitionId) && c.Index >= fromLowerIndexInclusive && c.Index <= toUpperIndexInclusive)
                        .OrderBy(c => c.Index);

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

            public Task ReadBackwardAsync(string partitionId, long fromUpperIndexInclusive, ISubscription subscription, long toLowerIndexInclusive, int limit, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = _chunks
                        .Where(c => c.PartitionId == partitionId && c.Index <= fromUpperIndexInclusive && c.Index >= toLowerIndexInclusive)
                        .OrderByDescending(c => c.Index)
                        .Take(limit);

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

            public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var chunk = _chunks
                        .Where(c => c.PartitionId == partitionId && c.Index <= fromUpperIndexInclusive)
                        .OrderByDescending(c => c.Index)
                        .FirstOrDefault();
                    return Task.FromResult<IChunk>(chunk);
                }
            }

            public Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = _chunks
                        .Where(c => c.Position >= fromPositionInclusive)
                        .OrderBy(c => c.Position)
                        .Take(limit);

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

            public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    return Task.FromResult(_position);
                }
            }

            public Task<IChunk> ReplaceOneAsync(long position, string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var chunk = _chunks.FirstOrDefault(c => c.Position == position);
                    if (chunk != null)
                    {
                        chunk.Payload = payload;
                        chunk.OperationId = operationId;
                    }
                    return Task.FromResult<IChunk>(chunk);
                }
            }

            public Task<IChunk> ReadOneAsync(long position, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var chunk = _chunks.FirstOrDefault(c => c.Position == position);
                    return Task.FromResult<IChunk>(chunk);
                }
            }

            public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    _chunks.RemoveAll(c => c.PartitionId == partitionId && c.Index >= fromLowerIndexInclusive && c.Index <= toUpperIndexInclusive);
                }
                return Task.CompletedTask;
            }

            public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var chunk = _chunks.FirstOrDefault(c => c.PartitionId == partitionId && c.OperationId == operationId);
                    return Task.FromResult<IChunk>(chunk);
                }
            }

            public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = _chunks.Where(c => c.OperationId == operationId);

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

            public Task ReadForwardMultiplePartitionsWithRangesAsync(IEnumerable<PartitionReadRequest> partitionRequests, ISubscription subscription, CancellationToken cancellationToken)
            {
                lock (_lock)
                {
                    var items = new List<TestChunk>();
                    foreach (var request in partitionRequests)
                    {
                        var partitionItems = _chunks
                            .Where(c => c.PartitionId == request.PartitionId && 
                                       c.Index >= request.FromPartitionIndexInclusive && 
                                       c.Index <= request.ToPartitionIndexInclusive)
                            .OrderBy(c => c.Index);
                        items.AddRange(partitionItems);
                    }

                    // Sort by index across all partitions (no temporal ordering guarantee)
                    items = items.OrderBy(c => c.Index).ToList();

                    foreach (var item in items)
                    {
                        subscription.OnNextAsync(item).GetAwaiter().GetResult();
                    }
                }
                return Task.CompletedTask;
            }

#if NET8_0_OR_GREATER
            public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsWithRangesAsync(
                IEnumerable<PartitionReadRequest> partitionRequests,
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                var items = new List<TestChunk>();
                lock (_lock)
                {
                    foreach (var request in partitionRequests)
                    {
                        var partitionItems = _chunks
                            .Where(c => c.PartitionId == request.PartitionId && 
                                       c.Index >= request.FromPartitionIndexInclusive && 
                                       c.Index <= request.ToPartitionIndexInclusive)
                            .OrderBy(c => c.Index);
                        items.AddRange(partitionItems);
                    }

                    // Sort by index across all partitions (no temporal ordering guarantee)
                    items = items.OrderBy(c => c.Index).ToList();
                }

                await Task.Delay(0, cancellationToken);
                foreach (var item in items)
                {
                    yield return item;
                }
            }

            public async IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
                IEnumerable<string> partitionIdsList, 
                long fromLowerIndexInclusive,
                long toUpperIndexInclusive,
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
            {
                var items = new List<TestChunk>();
                lock (_lock)
                {
                    items = _chunks
                        .Where(c => partitionIdsList.Contains(c.PartitionId) && c.Index >= fromLowerIndexInclusive && c.Index <= toUpperIndexInclusive)
                        .OrderBy(c => c.Index)
                        .ToList();
                }

                await Task.Delay(0, cancellationToken);
                foreach (var item in items)
                {
                    yield return item;
                }
            }
#endif

            private class TestChunk : IChunk
            {
                public long Position { get; set; }
                public string PartitionId { get; set; }
                public long Index { get; set; }
                public object Payload { get; set; }
                public string OperationId { get; set; }
            }
        }
    }
}
