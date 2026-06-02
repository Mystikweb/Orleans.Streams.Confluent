using Orleans.Configuration;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Options used to configure the Kafka stream provider.
/// </summary>
public sealed class KafkaStreamProviderOptions
{
    /// <summary>
    /// Gets or sets a semicolon-delimited Kafka client connection string.
    /// Expected format is key-value pairs like:
    /// bootstrap.servers=host:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the Kafka bootstrap servers.
    /// This value overrides bootstrap.servers from <see cref="ConnectionString"/> when both are provided.
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
