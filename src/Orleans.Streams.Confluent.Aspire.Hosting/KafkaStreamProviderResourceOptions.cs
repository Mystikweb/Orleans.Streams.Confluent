namespace Orleans.Streams.Confluent.Aspire.Hosting;

/// <summary>
/// AppHost modeling options for a Confluent Kafka stream provider configured for Orleans.
/// </summary>
public sealed class KafkaStreamProviderResourceOptions
{
    /// <summary>
    /// Gets or sets a semicolon-delimited Kafka client connection string.
    /// </summary>
    public string? ConnectionString { get; init; }

    /// <summary>
    /// Gets or sets the Kafka bootstrap servers.
    /// </summary>
    public string? BootstrapServers { get; init; }

    /// <summary>
    /// Gets or sets the topic name used by the provider.
    /// </summary>
    public required string TopicName { get; init; }

    /// <summary>
    /// Gets or sets the number of partitions to create when the topic is provisioned.
    /// </summary>
    public int? PartitionCount { get; init; }

    /// <summary>
    /// Gets or sets the replication factor used when creating the topic.
    /// </summary>
    public short? ReplicationFactor { get; init; }

    /// <summary>
    /// Gets or sets a value indicating whether the topic should be created if it does not exist.
    /// </summary>
    public bool? CreateTopicIfMissing { get; init; }

    /// <summary>
    /// Gets or sets the consumer group prefix used for generated Kafka consumer group names.
    /// </summary>
    public string? ConsumerGroupPrefix { get; init; }
}
