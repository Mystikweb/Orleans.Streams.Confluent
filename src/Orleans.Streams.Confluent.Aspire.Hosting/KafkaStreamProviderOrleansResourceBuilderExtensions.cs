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
    /// <param name="service">The Orleans service model to configure.</param>
    /// <param name="providerName">The Orleans stream provider name.</param>
    /// <param name="options">The Kafka stream provider options to project into Orleans configuration.</param>
    /// <returns>The updated Orleans service model.</returns>
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
        ValidateTopicProvisioningValues(options.PartitionCount, options.ReplicationFactor);

        return service.WithStreaming(providerName, new KafkaStreamProviderConfiguration(options));
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using a resource
    /// that exposes a connection string expression. This allows BootstrapServers to be
    /// resolved from AppHost resource wiring instead of a literal value.
    /// </summary>
    /// <param name="service">The Orleans service model to configure.</param>
    /// <param name="providerName">The Orleans stream provider name.</param>
    /// <param name="kafkaResource">The resource builder whose resource exposes a connection string expression.</param>
    /// <param name="topicName">The Kafka topic name used by the stream provider.</param>
    /// <param name="partitionCount">Optional Kafka partition count to configure.</param>
    /// <param name="replicationFactor">Optional Kafka replication factor to configure.</param>
    /// <param name="createTopicIfMissing">Optional flag controlling whether the topic should be created when missing.</param>
    /// <param name="consumerGroupPrefix">Optional prefix used for generated Kafka consumer group names.</param>
    /// <returns>The updated Orleans service model.</returns>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        IResourceBuilder<IResourceWithConnectionString> kafkaResource,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null,
        string? consumerGroupPrefix = null)
    {
        ArgumentNullException.ThrowIfNull(service);
        ArgumentNullException.ThrowIfNull(kafkaResource);

        return service.WithKafkaStreamProvider(
            providerName,
            kafkaResource.Resource,
            topicName,
            partitionCount,
            replicationFactor,
            createTopicIfMissing,
            consumerGroupPrefix);
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using an existing
    /// connection-string resource. This supports resources added via AddConnectionString and
    /// other integrations that implement <see cref="IResourceWithConnectionString"/>.
    /// </summary>
    /// <param name="service">The Orleans service model to configure.</param>
    /// <param name="providerName">The Orleans stream provider name.</param>
    /// <param name="kafkaResource">The resource that exposes a Kafka connection string expression.</param>
    /// <param name="topicName">The Kafka topic name used by the stream provider.</param>
    /// <param name="partitionCount">Optional Kafka partition count to configure.</param>
    /// <param name="replicationFactor">Optional Kafka replication factor to configure.</param>
    /// <param name="createTopicIfMissing">Optional flag controlling whether the topic should be created when missing.</param>
    /// <param name="consumerGroupPrefix">Optional prefix used for generated Kafka consumer group names.</param>
    /// <returns>The updated Orleans service model.</returns>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        IResourceWithConnectionString kafkaResource,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null,
        string? consumerGroupPrefix = null)
    {
        ArgumentNullException.ThrowIfNull(service);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(kafkaResource);
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ValidateTopicProvisioningValues(partitionCount, replicationFactor);

        return service.WithStreaming(
            providerName,
            new KafkaStreamProviderConfiguration(
                kafkaResource.ConnectionStringExpression,
                topicName,
                partitionCount,
                replicationFactor,
                createTopicIfMissing,
                consumerGroupPrefix));
    }

    private static void ValidateTopicProvisioningValues(int? partitionCount, short? replicationFactor)
    {
        if (partitionCount is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount), partitionCount, "PartitionCount must be greater than zero when specified.");
        }

        if (replicationFactor is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(replicationFactor), replicationFactor, "ReplicationFactor must be greater than zero when specified.");
        }
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model.
    /// The provider configuration is propagated to silo and client projects via
    /// <see cref="Aspire.Hosting.OrleansServiceExtensions.WithReference{T}" />.
    /// </summary>
    /// <param name="service">The Orleans service model to configure.</param>
    /// <param name="providerName">The Orleans stream provider name.</param>
    /// <param name="bootstrapServers">The Kafka bootstrap servers value.</param>
    /// <param name="topicName">The Kafka topic name used by the stream provider.</param>
    /// <param name="partitionCount">Optional Kafka partition count to configure.</param>
    /// <param name="replicationFactor">Optional Kafka replication factor to configure.</param>
    /// <param name="createTopicIfMissing">Optional flag controlling whether the topic should be created when missing.</param>
    /// <param name="consumerGroupPrefix">Optional prefix used for generated Kafka consumer group names.</param>
    /// <returns>The updated Orleans service model.</returns>
    public static OrleansService WithKafkaStreamProvider(
        this OrleansService service,
        string providerName,
        string bootstrapServers,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null,
        string? consumerGroupPrefix = null)
    {
        return service.WithKafkaStreamProvider(
            providerName,
            new KafkaStreamProviderResourceOptions
            {
                BootstrapServers = bootstrapServers,
                TopicName = topicName,
                PartitionCount = partitionCount,
                ReplicationFactor = replicationFactor,
                CreateTopicIfMissing = createTopicIfMissing,
                ConsumerGroupPrefix = consumerGroupPrefix
            });
    }

    /// <summary>
    /// Adds a Confluent Kafka stream provider to the Orleans service model using a connection string.
    /// </summary>
    /// <param name="service">The Orleans service model to configure.</param>
    /// <param name="providerName">The Orleans stream provider name.</param>
    /// <param name="connectionString">The Kafka client connection string.</param>
    /// <param name="topicName">The Kafka topic name used by the stream provider.</param>
    /// <param name="partitionCount">Optional Kafka partition count to configure.</param>
    /// <param name="replicationFactor">Optional Kafka replication factor to configure.</param>
    /// <param name="createTopicIfMissing">Optional flag controlling whether the topic should be created when missing.</param>
    /// <param name="consumerGroupPrefix">Optional prefix used for generated Kafka consumer group names.</param>
    /// <returns>The updated Orleans service model.</returns>
    public static OrleansService WithKafkaStreamProviderConnectionString(
        this OrleansService service,
        string providerName,
        string connectionString,
        string topicName,
        int? partitionCount = null,
        short? replicationFactor = null,
        bool? createTopicIfMissing = null,
        string? consumerGroupPrefix = null)
    {
        return service.WithKafkaStreamProvider(
            providerName,
            new KafkaStreamProviderResourceOptions
            {
                ConnectionString = connectionString,
                TopicName = topicName,
                PartitionCount = partitionCount,
                ReplicationFactor = replicationFactor,
                CreateTopicIfMissing = createTopicIfMissing,
                ConsumerGroupPrefix = consumerGroupPrefix
            });
    }
}
