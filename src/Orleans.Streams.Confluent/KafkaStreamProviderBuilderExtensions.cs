using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Serialization;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Extensions for registering the Kafka stream provider components.
/// </summary>
public static class KafkaStreamProviderBuilderExtensions
{
    /// <summary>
    /// Registers the Kafka stream provider infrastructure and options.
    /// </summary>
    public static ISiloBuilder AddKafkaStreamProvider(this ISiloBuilder builder, string providerName, Action<KafkaStreamProviderOptions>? configureOptions = null, int partitionCount = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);

        builder.Services.AddOptions<KafkaStreamProviderOptions>(providerName).Configure(options =>
        {
            options.PartitionCount = partitionCount;
            configureOptions?.Invoke(options);
        });

        builder.Services.AddSingleton<IQueueAdapterFactory>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get(providerName);
            var queueMapperOptions = serviceProvider.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(providerName);
            var cacheOptions = serviceProvider.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(providerName);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var serializer = serviceProvider.GetRequiredService<Serializer<KafkaBatchContainer>>();
            return new KafkaQueueAdapterFactory(providerName, options, queueMapperOptions, cacheOptions, loggerFactory, serializer);
        });

        return builder;
    }
}
