using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Extensions for registering the Kafka stream provider components.
/// </summary>
public static class KafkaStreamProviderBuilderExtensions
{
    /// <summary>
    /// Registers the Kafka stream provider infrastructure and options.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="providerName">The logical stream provider name used for named options and stream provider resolution.</param>
    /// <param name="configureOptions">Optional callback to configure Kafka provider options.</param>
    /// <param name="partitionCount">The default partition count assigned before <paramref name="configureOptions"/> runs.</param>
    /// <returns>The updated silo builder.</returns>
    public static ISiloBuilder AddKafkaStreamProvider(this ISiloBuilder builder, string providerName, Action<KafkaStreamProviderOptions>? configureOptions = null, int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        builder.Services.AddOptions<KafkaStreamProviderOptions>(providerName).Configure(options =>
        {
            options.PartitionCount = partitionCount;
            configureOptions?.Invoke(options);
        });

        builder.AddPersistentStreams(providerName, CreateQueueAdapterFactory, _ =>
        {
        });

        return builder;
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
}
