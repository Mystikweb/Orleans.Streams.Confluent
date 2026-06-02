using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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

        if (string.Equals(sectionPath, "Orleans:Streams:Kafka", StringComparison.Ordinal))
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

        if (string.Equals(sectionPath, "Orleans:Streams:Kafka", StringComparison.Ordinal))
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
        return section.Value is not null || section.GetChildren().Any();
    }

    private static void RegisterKafkaStreamProvider(
        IServiceCollection services,
        string providerName,
        Action<KafkaStreamProviderOptions> configureOptions,
        int partitionCount)
    {
        services.AddOptions<KafkaStreamProviderOptions>(providerName).Configure(options =>
        {
            options.PartitionCount = partitionCount;
            configureOptions.Invoke(options);
        });

        services.AddSingleton<IQueueAdapterFactory>(serviceProvider => CreateQueueAdapterFactory(serviceProvider, providerName));

    }

    private static IQueueAdapterFactory CreateQueueAdapterFactory(IServiceProvider serviceProvider, string streamProviderName)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get(streamProviderName);
        var queueMapperOptions = serviceProvider.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(streamProviderName);
        var cacheOptions = serviceProvider.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(streamProviderName);
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        var serializer = serviceProvider.GetRequiredService<Serializer<KafkaBatchContainer>>();
        return new KafkaQueueAdapterFactory(streamProviderName, options, queueMapperOptions, cacheOptions, loggerFactory, serializer);
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

        if (int.TryParse(configuration[nameof(KafkaStreamProviderOptions.PartitionCount)], out var configuredPartitionCount) && configuredPartitionCount > 0)
        {
            options.PartitionCount = configuredPartitionCount;
        }

        if (short.TryParse(configuration[nameof(KafkaStreamProviderOptions.ReplicationFactor)], out var replicationFactor) && replicationFactor > 0)
        {
            options.ReplicationFactor = replicationFactor;
        }

        if (bool.TryParse(configuration[nameof(KafkaStreamProviderOptions.CreateTopicIfMissing)], out var createTopicIfMissing))
        {
            options.CreateTopicIfMissing = createTopicIfMissing;
        }
    }
}
