using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Testcontainers.Kafka;

namespace Orleans.Streams.Confluent.IntegrationTests;

[TestClass]
public sealed class KafkaStreamProviderIntegrationTests
{
    [TestMethod]
    public async Task AddKafkaStreamProvider_WhenPartitionCountConfigured_CreatesTopicAndPublishesAndReceivesMessages()
    {
        const int partitionCount = 3;
        var providerName = $"kafka-{Guid.NewGuid():N}";
        var topicName = $"orders-{Guid.NewGuid():N}";

        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<Orleans.Configuration.ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName,
                    options =>
                    {
                        options.BootstrapServers = kafka.GetBootstrapAddress();
                        options.TopicName = topicName;
                        options.CreateTopicIfMissing = true;
                        options.ReplicationFactor = 1;
                    },
                    partitionCount);
            })
            .Build();

        await using var factory = CreateFactory(host.Services, providerName);
        var adapter = await factory.CreateAdapter();
        var serializer = host.Services.GetRequiredService<Serializer<KafkaBatchContainer>>();
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.GetBootstrapAddress() }).Build();
        var topicMetadata = await WaitForTopicAsync(adminClient, topicName, partitionCount);
        topicMetadata.Partitions.Should().HaveCount(partitionCount);

        var streamId = StreamId.Create("orders", Guid.NewGuid().ToString("N"));
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        await adapter.QueueMessageBatchAsync(
            streamId,
            new[] { "created" },
            new EventSequenceTokenV2(42),
            null!);

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.GetBootstrapAddress(),
            GroupId = $"assert-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        }).Build();
        consumer.Assign(new TopicPartition(topicName, new Partition((int)queueId.GetNumericId())));

        var consumedMessage = await WaitForKafkaMessageAsync(consumer);
        var batch = KafkaBatchContainer.FromPayload(serializer, consumedMessage.Message.Value);
        batch.Topic.Should().Be(topicName);
        batch.Partition.Should().Be((int)queueId.GetNumericId());
        batch.SequenceToken.Should().BeOfType<EventSequenceTokenV2>();
        ((EventSequenceTokenV2)batch.SequenceToken).SequenceNumber.Should().Be(42);
        batch.GetEvents<string>().Select(tuple => tuple.Item1).Should().ContainSingle().Which.Should().Be("created");
    }

    [TestMethod]
    public async Task KafkaQueueAdapterReceiver_WhenInitializedBeforePublish_ReadsNewlyProducedBatch()
    {
        const int partitionCount = 3;
        var providerName = $"kafka-{Guid.NewGuid():N}";
        var topicName = $"orders-{Guid.NewGuid():N}";

        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<Orleans.Configuration.ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName,
                    options =>
                    {
                        options.BootstrapServers = kafka.GetBootstrapAddress();
                        options.TopicName = topicName;
                        options.CreateTopicIfMissing = true;
                        options.ReplicationFactor = 1;
                    },
                    partitionCount);
            })
            .Build();

        await using var factory = CreateFactory(host.Services, providerName);
        var adapter = await factory.CreateAdapter();
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.GetBootstrapAddress() }).Build();
        var topicMetadata = await WaitForTopicAsync(adminClient, topicName, partitionCount);
        topicMetadata.Partitions.Should().HaveCount(partitionCount);

        var streamId = StreamId.Create("orders", Guid.NewGuid().ToString("N"));
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);
        var receiver = adapter.CreateReceiver(queueId);

        await receiver.Initialize(TimeSpan.FromSeconds(5));

        await adapter.QueueMessageBatchAsync(
            streamId,
            new[] { "created" },
            new EventSequenceTokenV2(0),
            new Dictionary<string, object>());

        var messages = await WaitForReceiverMessagesAsync(receiver);
        messages.Should().ContainSingle();

        var batch = messages[0].Should().BeOfType<KafkaBatchContainer>().Subject;
        batch.Topic.Should().Be(topicName);
        batch.Partition.Should().Be((int)queueId.GetNumericId());
        batch.GetEvents<string>().Select(tuple => tuple.Item1).Should().ContainSingle().Which.Should().Be("created");

        await receiver.MessagesDeliveredAsync(messages);
        await receiver.Shutdown(TimeSpan.FromSeconds(5));
    }

    [TestMethod]
    public async Task KafkaQueueAdapterReceiver_WhenShutdown_GetQueueMessagesAsync_ReturnsEmpty()
    {
        const int partitionCount = 3;
        var providerName = $"kafka-{Guid.NewGuid():N}";
        var topicName = $"orders-{Guid.NewGuid():N}";

        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<Orleans.Configuration.ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName,
                    options =>
                    {
                        options.BootstrapServers = kafka.GetBootstrapAddress();
                        options.TopicName = topicName;
                        options.CreateTopicIfMissing = true;
                        options.ReplicationFactor = 1;
                    },
                    partitionCount);
            })
            .Build();

        await using var factory = CreateFactory(host.Services, providerName);
        var adapter = await factory.CreateAdapter();
        var streamId = StreamId.Create("orders", Guid.NewGuid().ToString("N"));
        var queueId = factory.GetStreamQueueMapper().GetQueueForStream(streamId);

        var receiver = adapter.CreateReceiver(queueId);

        await receiver.Initialize(TimeSpan.FromSeconds(5));
        await receiver.Shutdown(TimeSpan.FromSeconds(5));

        var messages = await receiver.GetQueueMessagesAsync(10);
        messages.Should().BeEmpty();
    }

    [TestMethod]
    public async Task CreateAdapter_WhenTopicMissingAndAutoProvisioningDisabled_ThrowsInvalidOperationException()
    {
        const int partitionCount = 3;
        var providerName = $"kafka-{Guid.NewGuid():N}";
        var topicName = $"orders-{Guid.NewGuid():N}";

        await using var kafka = new KafkaBuilder().Build();
        await kafka.StartAsync();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<Orleans.Configuration.ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName,
                    options =>
                    {
                        options.BootstrapServers = kafka.GetBootstrapAddress();
                        options.TopicName = topicName;
                        options.CreateTopicIfMissing = false;
                        options.ReplicationFactor = 1;
                    },
                    partitionCount);
            })
            .Build();

        await using var factory = CreateFactory(host.Services, providerName);

        var act = () => factory.CreateAdapter();
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    private static async Task<TopicMetadata> WaitForTopicAsync(IAdminClient adminClient, string topicName, int partitionCount)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topic = metadata.Topics.FirstOrDefault(candidate => candidate.Topic == topicName);
            if (topic is not null && topic.Partitions.Count == partitionCount)
            {
                return topic;
            }

            await Task.Delay(250);
        }

        Assert.Fail($"Kafka topic '{topicName}' was not created with {partitionCount} partition(s) before the timeout elapsed.");
        return null!;
    }

    private static async Task<ConsumeResult<Ignore, byte[]>> WaitForKafkaMessageAsync(IConsumer<Ignore, byte[]> consumer)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var message = consumer.Consume(TimeSpan.FromMilliseconds(250));
            if (message is not null)
            {
                return message;
            }

            await Task.Delay(250);
        }

        Assert.Fail("Kafka broker did not return the published batch before the timeout elapsed.");
        return null!;
    }

    private static async Task<IList<IBatchContainer>> WaitForReceiverMessagesAsync(IQueueAdapterReceiver receiver)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var messages = await receiver.GetQueueMessagesAsync(10);
            if (messages.Count > 0)
            {
                return messages;
            }

            await Task.Delay(250);
        }

        Assert.Fail("Kafka receiver did not observe any batches before the timeout elapsed.");
        return null!;
    }

    private static KafkaQueueAdapterFactory CreateFactory(IServiceProvider serviceProvider, string providerName)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get(providerName);
        var queueMapperOptions = serviceProvider.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(providerName);
        var cacheOptions = serviceProvider.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(providerName);
        var loggerFactory = serviceProvider.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>();
        var serializer = serviceProvider.GetRequiredService<Serializer<KafkaBatchContainer>>();
        return new KafkaQueueAdapterFactory(providerName, options, queueMapperOptions, cacheOptions, loggerFactory, serializer);
    }
}
