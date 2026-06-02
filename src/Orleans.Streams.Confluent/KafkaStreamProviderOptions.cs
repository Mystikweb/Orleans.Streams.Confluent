using Orleans.Configuration;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Options used to configure the Kafka stream provider.
/// </summary>
public sealed class KafkaStreamProviderOptions
{
    /// <summary>
    /// Gets or sets the Kafka bootstrap servers.
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the topic name used by the provider.
    /// </summary>
    public string TopicName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the number of partitions to create when the topic is provisioned.
    /// </summary>
    public int PartitionCount { get; set; } = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES;

    /// <summary>
    /// Gets or sets the replication factor used when creating the topic.
    /// </summary>
    public short ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Gets or sets a value indicating whether the topic should be created if it does not exist.
    /// </summary>
    public bool CreateTopicIfMissing { get; set; } = true;
}
