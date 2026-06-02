using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Streams.Confluent;

/// <summary>
/// A Kafka-backed Orleans queue adapter.
/// </summary>
internal sealed partial class KafkaQueueAdapter(
    string providerName,
    KafkaStreamProviderOptions options,
    Serializer<KafkaBatchContainer> serializer,
    ILoggerFactory loggerFactory,
    HashRingBasedStreamQueueMapper streamQueueMapper) : IQueueAdapter
{
    private readonly ConcurrentDictionary<QueueId, KafkaQueueAdapterReceiver> _receivers = new();
    private readonly ILogger _logger = loggerFactory.CreateLogger<KafkaQueueAdapter>();

    public string Name => providerName;

    public bool IsRewindable => false;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var created = false;
        var receiver = _receivers.GetOrAdd(queueId, id =>
        {
            created = true;
            return CreateReceiverForQueue(id);
        });

        if (created)
        {
            LogDebugReceiverCreated(queueId);
        }
        else
        {
            LogDebugReceiverReused(queueId);
        }

        return receiver;
    }

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);

        try
        {
            var queueId = streamQueueMapper.GetQueueForStream(streamId);
            LogDebugQueueingMessageBatch(streamId, queueId, options.TopicName);

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
                AllowAutoCreateTopics = false
            };

            using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
            var payload = KafkaBatchContainer.ToPayload(serializer, streamId, events, requestContext, options.TopicName, (int)queueId.GetNumericId(), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            await producer.ProduceAsync(new TopicPartition(options.TopicName, new Partition((int)queueId.GetNumericId())), new Message<Null, byte[]> { Value = payload }).ConfigureAwait(false);
            producer.Flush(TimeSpan.FromSeconds(5));
            LogDebugQueuedMessageBatch(streamId, queueId, options.TopicName);
        }
        catch (Exception ex)
        {
            LogErrorQueueingMessageBatchFailed(streamId, options.TopicName, ex);
            throw;
        }
    }

    private KafkaQueueAdapterReceiver CreateReceiverForQueue(QueueId queueId)
        => new(providerName, options, serializer, loggerFactory.CreateLogger<KafkaQueueAdapterReceiver>(), queueId);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Created Kafka receiver for queue {QueueId}")]
    private partial void LogDebugReceiverCreated(QueueId queueId);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Reusing existing Kafka receiver for queue {QueueId}")]
    private partial void LogDebugReceiverReused(QueueId queueId);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "Queueing Kafka batch for stream {StreamId} to queue {QueueId} on topic {TopicName}")]
    private partial void LogDebugQueueingMessageBatch(StreamId streamId, QueueId queueId, string topicName);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Debug,
        Message = "Queued Kafka batch for stream {StreamId} to queue {QueueId} on topic {TopicName}")]
    private partial void LogDebugQueuedMessageBatch(StreamId streamId, QueueId queueId, string topicName);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Error,
        Message = "Failed to queue Kafka batch for stream {StreamId} on topic {TopicName}")]
    private partial void LogErrorQueueingMessageBatchFailed(StreamId streamId, string topicName, Exception exception);
}
