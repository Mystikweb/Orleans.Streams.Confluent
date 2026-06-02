using Aspire.Hosting;
using Aspire.Hosting.Orleans;

namespace Orleans.Streams.Confluent.Aspire.Hosting;

/// <summary>
/// AppHost extensions for modeling Confluent Kafka stream providers on an Orleans resource.
/// </summary>
public static class KafkaStreamProviderOrleansResourceBuilderExtensions
{
    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model.
    /// The provider configuration is propagated to silo and client projects via
    /// <see cref="Aspire.Hosting.OrleansServiceExtensions.WithReference{T}" />.
    /// </summary>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        KafkaStreamProviderResourceOptions options)
    {
        ArgumentNullException.ThrowIfNull(service);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.TopicName);

        return service.WithStreaming(providerName, new KafkaStreamProviderConfiguration(options));
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model.
    /// The provider configuration is propagated to silo and client projects via
    /// <see cref="Aspire.Hosting.OrleansServiceExtensions.WithReference{T}" />.
    /// </summary>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        string bootstrapServers,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null)
    {
        return service.WithKafkaStreamProvider(
            providerName,
            new KafkaStreamProviderResourceOptions
            {
                BootstrapServers = bootstrapServers,
                TopicName = topicName,
                PartitionCount = partitionCount,
                ReplicationFactor = replicationFactor,
                CreateTopicIfMissing = createTopicIfMissing
            });
    }
}
