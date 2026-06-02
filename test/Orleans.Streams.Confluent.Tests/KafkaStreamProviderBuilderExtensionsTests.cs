using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;

namespace Orleans.Streams.Confluent.Tests;

[TestClass]
public sealed class KafkaStreamProviderBuilderExtensionsTests
{
    [TestMethod]
    public void AddKafkaStreamProvider_WhenPartitionCountParameterIsNotPositive_ThrowsArgumentOutOfRangeException()
    {
        Action act = () =>
        {
            using var _ = new HostBuilder()
                .UseOrleans(silo =>
                {
                    silo.UseLocalhostClustering();
                    silo.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = Guid.NewGuid().ToString("N");
                        options.ServiceId = Guid.NewGuid().ToString("N");
                    });

                    silo.AddKafkaStreamProvider(
                        providerName: "kafka",
                        configureOptions: options =>
                        {
                            options.BootstrapServers = "localhost:9092";
                            options.TopicName = "orders-topic";
                        },
                        partitionCount: 0);
                })
                .Build();
        };

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenPartitionCountIsNotPositive_ThrowsArgumentOutOfRangeException()
    {
        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.BootstrapServers = "localhost:9092";
                        options.TopicName = "orders-topic";
                        options.PartitionCount = 0;
                    });
            })
            .Build();

        Action act = () => CreateFactory(host.Services, "kafka");

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenReplicationFactorIsNotPositive_ThrowsArgumentOutOfRangeException()
    {
        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.BootstrapServers = "localhost:9092";
                        options.TopicName = "orders-topic";
                        options.ReplicationFactor = 0;
                    });
            })
            .Build();

        Action act = () => CreateFactory(host.Services, "kafka");

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenProviderNameIsWhitespace_ThrowsArgumentException()
    {
        Action act = () =>
        {
            using var _ = new HostBuilder()
                .UseOrleans(silo =>
                {
                    silo.UseLocalhostClustering();
                    silo.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = Guid.NewGuid().ToString("N");
                        options.ServiceId = Guid.NewGuid().ToString("N");
                    });
                    silo.AddKafkaStreamProvider(" ");
                })
                .Build();
        };

        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenConfigured_RegistersFactoryAndNamedOptions()
    {
        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.BootstrapServers = "localhost:9092";
                        options.TopicName = "orders-topic";
                        options.CreateTopicIfMissing = false;
                    },
                    partitionCount: 6);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.CreateTopicIfMissing.Should().BeFalse();
        options.PartitionCount.Should().Be(6);

        var queueMapperOptions = host.Services.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get("kafka");
        queueMapperOptions.TotalQueueCount.Should().Be(6);
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenConnectionStringConfigured_RegistersFactoryAndNamedOptions()
    {
        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.ConnectionString = "bootstrap.servers=localhost:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN;sasl.username=key;sasl.password=secret";
                        options.TopicName = "orders-topic";
                        options.CreateTopicIfMissing = false;
                    },
                    partitionCount: 6);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.ConnectionString.Should().Contain("bootstrap.servers=localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.CreateTopicIfMissing.Should().BeFalse();
        options.PartitionCount.Should().Be(6);

        var queueMapperOptions = host.Services.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get("kafka");
        queueMapperOptions.TotalQueueCount.Should().Be(6);
    }

    [TestMethod]
    public void AddKafkaStreamProvider_OnClientBuilder_WhenConfigured_RegistersFactoryAndNamedOptions()
    {
        using var host = new HostBuilder()
            .UseOrleansClient(client =>
            {
                client.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.BootstrapServers = "localhost:9092";
                        options.TopicName = "orders-topic";
                        options.CreateTopicIfMissing = false;
                    },
                    partitionCount: 6);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.CreateTopicIfMissing.Should().BeFalse();
        options.PartitionCount.Should().Be(6);

        var queueMapperOptions = host.Services.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get("kafka");
        queueMapperOptions.TotalQueueCount.Should().Be(6);
    }

    [TestMethod]
    public void AddKafkaStreamProvider_WhenConfigureOptionsOverridesPartitionCount_QueueMapperMatchesFinalPartitionCount()
    {
        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProvider(
                    providerName: "kafka",
                    configureOptions: options =>
                    {
                        options.BootstrapServers = "localhost:9092";
                        options.TopicName = "orders-topic";
                        options.PartitionCount = 9;
                        options.CreateTopicIfMissing = false;
                    },
                    partitionCount: 6);
            })
            .Build();

        var options = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get("kafka");
        var queueMapperOptions = host.Services.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get("kafka");

        options.PartitionCount.Should().Be(9);
        queueMapperOptions.TotalQueueCount.Should().Be(9);
    }

    private static KafkaQueueAdapterFactory CreateFactory(IServiceProvider serviceProvider, string providerName)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>().Get(providerName);
        var queueMapperOptions = serviceProvider.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(providerName);
        var cacheOptions = serviceProvider.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(providerName);
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        var serializer = serviceProvider.GetRequiredService<Serializer<KafkaBatchContainer>>();
        return new KafkaQueueAdapterFactory(providerName, options, queueMapperOptions, cacheOptions, loggerFactory, serializer);
    }
}
