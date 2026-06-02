using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
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
        if (string.IsNullOrWhiteSpace(options.BootstrapServers) && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Either BootstrapServers or ConnectionString must be configured.", nameof(options));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(options.TopicName);

        return service.WithStreaming(providerName, new KafkaStreamProviderConfiguration(options));
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using a resource
    /// that exposes a connection string expression. This allows BootstrapServers to be
    /// resolved from AppHost resource wiring instead of a literal value.
    /// </summary>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        IResourceBuilder<IResourceWithConnectionString> kafkaResource,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null)
    {
        ArgumentNullException.ThrowIfNull(kafkaResource);

        return service.WithKafkaStreamProvider(
            providerName,
            kafkaResource.Resource,
            topicName,
            partitionCount,
            replicationFactor,
            createTopicIfMissing);
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using an existing
    /// connection-string resource. This supports resources added via AddConnectionString and
    /// other integrations that implement <see cref="IResourceWithConnectionString"/>.
    /// </summary>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        IResourceWithConnectionString kafkaResource,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null)
    {
        ArgumentNullException.ThrowIfNull(service);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(kafkaResource);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return service.WithStreaming(
            providerName,
            new KafkaStreamProviderConfiguration(
                kafkaResource.ConnectionStringExpression,
                topicName,
                partitionCount,
                replicationFactor,
                createTopicIfMissing));
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

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using a connection string.
    /// </summary>
    public static OrleansService WithKafkaStreamProviderConnectionString(
        this OrleansService service,
        string providerName,
        string connectionString,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null)
    {
        return service.WithKafkaStreamProvider(
            providerName,
            new KafkaStreamProviderResourceOptions
            {
                ConnectionString = connectionString,
                TopicName = topicName,
                PartitionCount = partitionCount,
                ReplicationFactor = replicationFactor,
                CreateTopicIfMissing = createTopicIfMissing
            });
    }
}
