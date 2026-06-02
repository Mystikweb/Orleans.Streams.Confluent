using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;

namespace Orleans.Streams.Confluent;

/// <summary>
/// Factory for Kafka-backed Orleans queue adapters.
/// </summary>
public sealed partial class KafkaQueueAdapterFactory : IQueueAdapterFactory, IAsyncDisposable
{
    private readonly string _providerName;
    private readonly KafkaStreamProviderOptions _options;
    private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
    private readonly SimpleQueueAdapterCache _adapterCache;
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Serializer<KafkaBatchContainer> _serializer;

    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private IAdminClient? _adminClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaQueueAdapterFactory"/> class.
    /// </summary>
    public KafkaQueueAdapterFactory(
        string providerName,
        KafkaStreamProviderOptions options,
        HashRingStreamQueueMapperOptions queueMapperOptions,
        SimpleQueueCacheOptions cacheOptions,
        ILoggerFactory loggerFactory,
        Serializer<KafkaBatchContainer> serializer)
    {
        _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = _loggerFactory.CreateLogger<KafkaQueueAdapterFactory>();
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        queueMapperOptions = queueMapperOptions ?? throw new ArgumentNullException(nameof(queueMapperOptions));

        if (string.IsNullOrWhiteSpace(_options.TopicName))
        {
            throw new ArgumentException("Kafka topic name must be configured.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(KafkaClientConfigurationBuilder.ResolveBootstrapServers(_options)))
        {
            throw new ArgumentException("Kafka bootstrap servers must be configured via BootstrapServers or ConnectionString.", nameof(options));
        }


        queueMapperOptions.TotalQueueCount = _options.PartitionCount;
        _streamQueueMapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, _providerName);
        _adapterCache = new SimpleQueueAdapterCache(cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions)), _providerName, loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory)));
    }

    /// <inheritdoc />
    public async Task<IQueueAdapter> CreateAdapter()
    {
        try
        {
            LogDebugCreatingAdapter(_providerName, _options.TopicName, _options.PartitionCount);
            await EnsureTopicAsync().ConfigureAwait(false);
            var adapter = new KafkaQueueAdapter(_providerName, _options, _serializer, _loggerFactory, _streamQueueMapper);
            LogDebugAdapterCreated(_providerName, _options.TopicName);
            return adapter;
        }
        catch (Exception ex)
        {
            LogErrorCreateAdapterFailed(_providerName, _options.TopicName, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());

    /// <inheritdoc />
    public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

    /// <inheritdoc />
    public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

    private async Task EnsureTopicAsync()
    {
        if (!_options.CreateTopicIfMissing)
        {
            LogDebugTopicProvisioningSkipped(_providerName, _options.TopicName);
            return;
        }

        await _initializationLock.WaitAsync().ConfigureAwait(false);
        try
        {
            LogDebugEnsuringTopic(_providerName, _options.TopicName, _options.PartitionCount);
            _adminClient ??= new AdminClientBuilder(KafkaClientConfigurationBuilder.CreateAdminClientConfig(_options)).Build();

            var metadata = _adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var existingTopic = metadata.Topics.FirstOrDefault(topic => topic.Topic == _options.TopicName && topic.Error.IsError is false);
            if (existingTopic is not null)
            {
                if (existingTopic.Partitions.Count != _options.PartitionCount)
                {
                    throw new InvalidOperationException($"Kafka topic '{_options.TopicName}' exists with {existingTopic.Partitions.Count} partition(s) but provider '{_providerName}' is configured for {_options.PartitionCount} partition(s).");
                }

                LogDebugTopicAlreadyExists(_options.TopicName);
                return;
            }

            try
            {
                await _adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = _options.TopicName,
                        NumPartitions = _options.PartitionCount,
                        ReplicationFactor = _options.ReplicationFactor
                    }
                ]).ConfigureAwait(false);

                await WaitForTopicPartitionsAsync(_options.TopicName, _options.PartitionCount).ConfigureAwait(false);

                LogInformationTopicCreated(_options.TopicName, _options.PartitionCount, _options.ReplicationFactor);
            }
            catch (CreateTopicsException ex) when (ex.Results.Count == 1 && ex.Results[0].Error.Code == global::Confluent.Kafka.ErrorCode.TopicAlreadyExists)
            {
                LogDebugTopicAlreadyExists(_options.TopicName);
            }
            catch (Exception ex)
            {
                LogErrorEnsureTopicFailed(_options.TopicName, ex);
                throw;
            }
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    private async Task WaitForTopicPartitionsAsync(string topicName, int partitionCount)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var metadata = _adminClient!.GetMetadata(TimeSpan.FromSeconds(5));
            var topic = metadata.Topics.FirstOrDefault(candidate => candidate.Topic == topicName);
            if (topic is not null && topic.Partitions.Count == partitionCount)
            {
                return;
            }

            await Task.Delay(250).ConfigureAwait(false);
        }

        throw new TimeoutException($"Kafka topic '{topicName}' was not fully provisioned with {partitionCount} partition(s) before the timeout elapsed.");
    }

    public ValueTask DisposeAsync()
    {
        _adminClient?.Dispose();
        _initializationLock.Dispose();
        return ValueTask.CompletedTask;
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Creating Kafka adapter for provider {ProviderName} on topic {TopicName} with {PartitionCount} partition(s)")]
    private partial void LogDebugCreatingAdapter(string providerName, string topicName, int partitionCount);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Created Kafka adapter for provider {ProviderName} on topic {TopicName}")]
    private partial void LogDebugAdapterCreated(string providerName, string topicName);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "Skipping Kafka topic provisioning for provider {ProviderName} on topic {TopicName}")]
    private partial void LogDebugTopicProvisioningSkipped(string providerName, string topicName);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Debug,
        Message = "Ensuring Kafka topic {TopicName} exists for provider {ProviderName} with {PartitionCount} partition(s)")]
    private partial void LogDebugEnsuringTopic(string providerName, string topicName, int partitionCount);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Debug,
        Message = "Kafka topic {TopicName} already exists")]
    private partial void LogDebugTopicAlreadyExists(string topicName);

    [LoggerMessage(
        EventId = 6,
        Level = LogLevel.Information,
        Message = "Created Kafka topic {TopicName} with {PartitionCount} partition(s) and replication factor {ReplicationFactor}")]
    private partial void LogInformationTopicCreated(string topicName, int partitionCount, short replicationFactor);

    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Error,
        Message = "Failed to create Kafka adapter for provider {ProviderName} on topic {TopicName}")]
    private partial void LogErrorCreateAdapterFailed(string providerName, string topicName, Exception exception);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Error,
        Message = "Failed to ensure Kafka topic {TopicName} exists")]
    private partial void LogErrorEnsureTopicFailed(string topicName, Exception exception);
}
