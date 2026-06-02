using System.Globalization;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Orleans;

namespace Orleans.Streams.Confluent.Aspire.Hosting;

/// <summary>
/// Implements <see cref="IProviderConfiguration" /> for a Confluent Kafka stream provider.
/// Orleans Aspire calls <see cref="ConfigureResource{T}" /> for each registered provider
/// when a project references the Orleans service via <c>WithReference(orleans)</c>.
/// </summary>
internal sealed class KafkaStreamProviderConfiguration(KafkaStreamProviderResourceOptions options) : IProviderConfiguration
{
    private readonly ReferenceExpression? _connectionStringExpression;

    internal KafkaStreamProviderConfiguration(
        ReferenceExpression connectionStringExpression,
        string topicName,
        int? partitionCount,
        short? replicationFactor,
        bool? createTopicIfMissing)
        : this(new KafkaStreamProviderResourceOptions
        {
            ConnectionString = string.Empty,
            TopicName = topicName,
            PartitionCount = partitionCount,
            ReplicationFactor = replicationFactor,
            CreateTopicIfMissing = createTopicIfMissing
        })
    {
        _connectionStringExpression = connectionStringExpression;
    }

    /// <inheritdoc />
    void IProviderConfiguration.ConfigureResource<T>(IResourceBuilder<T> resourceBuilder, string configSectionPath)
    {
        // configSectionPath is the section Orleans passes for this streaming provider entry.
        // We emit our Kafka-specific keys as children of that section using the __ separator
        // convention so .NET configuration picks them up under Orleans:Streams:Kafka:{providerName}.
        var prefix = configSectionPath.Replace(":", "__", StringComparison.Ordinal) + "__";

        if (_connectionStringExpression is not null)
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.ConnectionString), _connectionStringExpression);
        }
        else if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.ConnectionString), options.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.BootstrapServers), options.BootstrapServers);
        }

        resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.TopicName), options.TopicName);

        if (options.PartitionCount.HasValue)
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.PartitionCount), options.PartitionCount.Value.ToString(CultureInfo.InvariantCulture));
        }

        if (options.ReplicationFactor.HasValue)
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.ReplicationFactor), options.ReplicationFactor.Value.ToString(CultureInfo.InvariantCulture));
        }

        if (options.CreateTopicIfMissing.HasValue)
        {
            resourceBuilder.WithEnvironment(prefix + nameof(KafkaStreamProviderResourceOptions.CreateTopicIfMissing), options.CreateTopicIfMissing.Value.ToString());
        }
    }
}
