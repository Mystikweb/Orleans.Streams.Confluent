using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Receives Kafka messages for a single Orleans queue partition.
/// </summary>
internal sealed partial class KafkaQueueAdapterReceiver(string providerName, KafkaStreamProviderOptions options, Serializer<KafkaBatchContainer> serializer, ILogger logger, QueueId queueId) : IQueueAdapterReceiver
{
    private IConsumer<Ignore, byte[]>? _consumer;

    public Task Initialize(TimeSpan timeout)
    {
        try
        {
            var consumerGroup = $"{providerName}-{queueId.GetNumericId()}";
            LogDebugInitializingReceiver(queueId, options.TopicName, consumerGroup);

            var consumerConfig = KafkaClientConfigurationBuilder.CreateConsumerConfig(options, consumerGroup);

            _consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build();
            var topicPartition = new TopicPartition(options.TopicName, new Partition((int)queueId.GetNumericId()));
            var watermarkOffsets = _consumer.QueryWatermarkOffsets(topicPartition, timeout);
            _consumer.Assign(new TopicPartitionOffset(topicPartition, watermarkOffsets.High));
            LogDebugReceiverAssigned(queueId, options.TopicName, queueId.GetNumericId());
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            LogErrorReceiverInitializationFailed(queueId, options.TopicName, ex);
            throw;
        }
    }

    public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        if (_consumer is null)
        {
            LogDebugReceiverNotInitialized(queueId);
            return Task.FromResult<IList<IBatchContainer>>([]);
        }

        try
        {
            var batches = new List<IBatchContainer>(maxCount);
            while (batches.Count < maxCount)
            {
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(50));
                if (result is null)
                {
                    break;
                }

                var container = KafkaBatchContainer.FromPayload(serializer, result.Message.Value);
                batches.Add(container);
            }

            if (batches.Count > 0)
            {
                LogDebugMessagesReceived(queueId, batches.Count);
            }

            return Task.FromResult<IList<IBatchContainer>>(batches);
        }
        catch (Exception ex)
        {
            LogErrorReceivingMessagesFailed(queueId, maxCount, ex);
            throw;
        }
    }

    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (_consumer is null || messages.Count == 0)
        {
            return Task.CompletedTask;
        }

        try
        {
            _consumer.Commit();
            LogDebugMessagesCommitted(queueId, messages.Count);
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            LogErrorCommitFailed(queueId, messages.Count, ex);
            throw;
        }
    }

    public Task Shutdown(TimeSpan timeout)
    {
        try
        {
            LogDebugShuttingDownReceiver(queueId);
            _consumer?.Dispose();
            _consumer = null;
            return Task.CompletedTask;
        }
        catch (Exception ex)
        {
            LogErrorShutdownFailed(queueId, ex);
            throw;
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Initializing Kafka receiver for queue {QueueId} on topic {TopicName} with consumer group {ConsumerGroup}")]
    private partial void LogDebugInitializingReceiver(QueueId queueId, string topicName, string consumerGroup);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Kafka receiver assigned queue {QueueId} to topic {TopicName} partition {PartitionId}")]
    private partial void LogDebugReceiverAssigned(QueueId queueId, string topicName, uint partitionId);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "Kafka receiver for queue {QueueId} was asked to read before initialization completed")]
    private partial void LogDebugReceiverNotInitialized(QueueId queueId);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Debug,
        Message = "Kafka receiver read {BatchCount} batch(es) from queue {QueueId}")]
    private partial void LogDebugMessagesReceived(QueueId queueId, int batchCount);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Debug,
        Message = "Kafka receiver committed {BatchCount} delivered batch(es) for queue {QueueId}")]
    private partial void LogDebugMessagesCommitted(QueueId queueId, int batchCount);

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Debug,
        Message = "Shutting down Kafka receiver for queue {QueueId}")]
    private partial void LogDebugShuttingDownReceiver(QueueId queueId);

    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Error,
        Message = "Kafka receiver initialization failed for queue {QueueId} on topic {TopicName}")]
    private partial void LogErrorReceiverInitializationFailed(QueueId queueId, string topicName, Exception exception);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Error,
        Message = "Kafka receiver failed to read up to {MaxCount} batch(es) from queue {QueueId}")]
    private partial void LogErrorReceivingMessagesFailed(QueueId queueId, int maxCount, Exception exception);

    [LoggerMessage(
        EventId = 9,
        Level = LogLevel.Error,
        Message = "Kafka receiver failed to commit {BatchCount} delivered batch(es) for queue {QueueId}")]
    private partial void LogErrorCommitFailed(QueueId queueId, int batchCount, Exception exception);

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Error,
        Message = "Kafka receiver shutdown failed for queue {QueueId}")]
    private partial void LogErrorShutdownFailed(QueueId queueId, Exception exception);
}
