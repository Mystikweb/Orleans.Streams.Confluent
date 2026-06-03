using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streams.Confluent.Aspire;

/// <summary>
/// Extensions for wiring the Kafka stream provider from configuration sources used in Aspire-hosted applications.
/// </summary>
public static class KafkaStreamProviderAspireExtensions
{
    /// <summary>
    /// Registers the Kafka stream provider using values from the specified configuration section path.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The stream provider name used for Orleans provider resolution and named options binding.</param>
    /// <param name="configuration">The configuration root used to resolve provider settings.</param>
    /// <param name="sectionPath">
    /// The configuration section path to bind. When this value matches <c>Orleans:Streams:Kafka</c>,
    /// provider-scoped configuration at <c>Orleans:Streams:Kafka:{providerName}</c> takes precedence when present.
    /// </param>
    /// <param name="partitionCount">The default partition count assigned before configuration values are applied.</param>
    /// <returns>The updated silo builder.</returns>
    public static ISiloBuilder AddKafkaStreamProviderFromConfiguration(
        this ISiloBuilder builder,
        string providerName,
        IConfiguration configuration,
        string sectionPath = "Orleans:Streams:Kafka",
        int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        if (string.Equals(sectionPath, "Orleans:Streams:Kafka", StringComparison.OrdinalIgnoreCase))
        {
            var providerScopedSection = configuration.GetSection($"Orleans:Streams:Kafka:{providerName}");
            if (SectionHasValues(providerScopedSection))
            {
                return builder.AddKafkaStreamProviderFromConfiguration(providerName, providerScopedSection, partitionCount);
            }
        }

        return builder.AddKafkaStreamProviderFromConfiguration(providerName, configuration.GetSection(sectionPath), partitionCount);
    }

    /// <summary>
    /// Registers the Kafka stream provider using values from the specified configuration section path.
    /// </summary>
    /// <param name="builder">The Orleans client builder.</param>
    /// <param name="providerName">The stream provider name used for Orleans provider resolution and named options binding.</param>
    /// <param name="configuration">The configuration root used to resolve provider settings.</param>
    /// <param name="sectionPath">
    /// The configuration section path to bind. When this value matches <c>Orleans:Streams:Kafka</c>,
    /// provider-scoped configuration at <c>Orleans:Streams:Kafka:{providerName}</c> takes precedence when present.
    /// </param>
    /// <param name="partitionCount">The default partition count assigned before configuration values are applied.</param>
    /// <returns>The updated client builder.</returns>
    public static IClientBuilder AddKafkaStreamProviderFromConfiguration(
        this IClientBuilder builder,
        string providerName,
        IConfiguration configuration,
        string sectionPath = "Orleans:Streams:Kafka",
        int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        if (string.Equals(sectionPath, "Orleans:Streams:Kafka", StringComparison.OrdinalIgnoreCase))
        {
            var providerScopedSection = configuration.GetSection($"Orleans:Streams:Kafka:{providerName}");
            if (SectionHasValues(providerScopedSection))
            {
                return builder.AddKafkaStreamProviderFromConfiguration(providerName, providerScopedSection, partitionCount);
            }
        }

        return builder.AddKafkaStreamProviderFromConfiguration(providerName, configuration.GetSection(sectionPath), partitionCount);
    }

    /// <summary>
    /// Registers the Kafka stream provider using values from a configuration section.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The stream provider name used for Orleans provider resolution and named options binding.</param>
    /// <param name="configurationSection">The configuration section containing Kafka stream provider settings.</param>
    /// <param name="partitionCount">The default partition count assigned before configuration values are applied.</param>
    /// <returns>The updated silo builder.</returns>
    public static ISiloBuilder AddKafkaStreamProviderFromConfiguration(
        this ISiloBuilder builder,
        string providerName,
        IConfigurationSection configurationSection,
        int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(configurationSection);

        RegisterKafkaStreamProvider(
            builder.Services,
            providerName,
            options => ApplyConfiguration(options, configurationSection),
            partitionCount);

        builder.AddPersistentStreams(providerName, CreateQueueAdapterFactory, _ =>
        {
        });

        return builder;
    }

    /// <summary>
    /// Registers the Kafka stream provider using values from a configuration section.
    /// </summary>
    /// <param name="builder">The Orleans client builder.</param>
    /// <param name="providerName">The stream provider name used for Orleans provider resolution and named options binding.</param>
    /// <param name="configurationSection">The configuration section containing Kafka stream provider settings.</param>
    /// <param name="partitionCount">The default partition count assigned before configuration values are applied.</param>
    /// <returns>The updated client builder.</returns>
    public static IClientBuilder AddKafkaStreamProviderFromConfiguration(
        this IClientBuilder builder,
        string providerName,
        IConfigurationSection configurationSection,
        int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        ArgumentNullException.ThrowIfNull(configurationSection);

        RegisterKafkaStreamProvider(
            builder.Services,
            providerName,
            options => ApplyConfiguration(options, configurationSection),
            partitionCount);

        builder.AddPersistentStreams(providerName, CreateQueueAdapterFactory, _ =>
        {
        });

        return builder;
    }

    private static bool SectionHasValues(IConfigurationSection section)
    {
        return !string.IsNullOrWhiteSpace(section.Value) || section.GetChildren().Any();
    }

    private static void RegisterKafkaStreamProvider(
        IServiceCollection services,
        string providerName,
        Action<KafkaStreamProviderOptions> configureOptions,
        int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(partitionCount, 0);

        services.AddOptions<KafkaStreamProviderOptions>(providerName).Configure(options =>
        {
            options.PartitionCount = partitionCount;
            configureOptions.Invoke(options);
        });

        services.AddOptions<HashRingStreamQueueMapperOptions>(providerName).Configure<IOptionsMonitor<KafkaStreamProviderOptions>>((options, kafkaOptionsMonitor) =>
        {
            options.TotalQueueCount = kafkaOptionsMonitor.Get(providerName).PartitionCount;
        });
    }

    private static IQueueAdapterFactory CreateQueueAdapterFactory(IServiceProvider serviceProvider, string streamProviderName)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get(streamProviderName);
        var queueMapperOptions = serviceProvider.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(streamProviderName);
        var cacheOptions = serviceProvider.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(streamProviderName);
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        var serializer = serviceProvider.GetRequiredService<Serializer<KafkaBatchContainer>>();
        var clusterOptions = serviceProvider.GetService<IOptions<ClusterOptions>>()?.Value;
        return new KafkaQueueAdapterFactory(streamProviderName, options, queueMapperOptions, cacheOptions, loggerFactory, serializer, clusterOptions);
    }

    private static void ApplyConfiguration(KafkaStreamProviderOptions options, IConfiguration configuration)
    {
        var connectionString = configuration[nameof(KafkaStreamProviderOptions.ConnectionString)];
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            options.ConnectionString = connectionString;
        }

        var bootstrapServers = configuration[nameof(KafkaStreamProviderOptions.BootstrapServers)];
        if (!string.IsNullOrWhiteSpace(bootstrapServers))
        {
            options.BootstrapServers = bootstrapServers;
        }

        var topicName = configuration[nameof(KafkaStreamProviderOptions.TopicName)];
        if (!string.IsNullOrWhiteSpace(topicName))
        {
            options.TopicName = topicName;
        }

        if (int.TryParse(configuration[nameof(KafkaStreamProviderOptions.PartitionCount)], NumberStyles.Integer, CultureInfo.InvariantCulture, out var configuredPartitionCount))
        {
            options.PartitionCount = configuredPartitionCount;
        }

        if (short.TryParse(configuration[nameof(KafkaStreamProviderOptions.ReplicationFactor)], NumberStyles.Integer, CultureInfo.InvariantCulture, out var replicationFactor))
        {
            options.ReplicationFactor = replicationFactor;
        }

        if (bool.TryParse(configuration[nameof(KafkaStreamProviderOptions.CreateTopicIfMissing)], out var createTopicIfMissing))
        {
            options.CreateTopicIfMissing = createTopicIfMissing;
        }

        var consumerGroupPrefix = configuration[nameof(KafkaStreamProviderOptions.ConsumerGroupPrefix)];
        if (!string.IsNullOrWhiteSpace(consumerGroupPrefix))
        {
            options.ConsumerGroupPrefix = consumerGroupPrefix;
        }
    }
}
