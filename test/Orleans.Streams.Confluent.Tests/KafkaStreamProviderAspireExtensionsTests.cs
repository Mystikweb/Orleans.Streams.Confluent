using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.Confluent.Aspire;

namespace Orleans.Streams.Confluent.Tests;

[TestClass]
public sealed class KafkaStreamProviderAspireExtensionsTests
{
    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_OnSiloBuilder_WhenProviderNameIsWhitespace_ThrowsArgumentException()
    {
        var configuration = new ConfigurationBuilder().Build();

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
                    silo.AddKafkaStreamProviderFromConfiguration(" ", configuration);
                })
                .Build();
        };

        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_OnClientBuilder_WhenProviderNameIsWhitespace_ThrowsArgumentException()
    {
        var configuration = new ConfigurationBuilder().Build();

        Action act = () =>
        {
            using var _ = new HostBuilder()
                .UseOrleansClient(client =>
                {
                    client.AddKafkaStreamProviderFromConfiguration(" ", configuration);
                })
                .Build();
        };

        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_WhenConfigured_BindsNamedOptions()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Orleans:Streams:Kafka:BootstrapServers"] = "localhost:9092",
                ["Orleans:Streams:Kafka:TopicName"] = "orders-topic",
                ["Orleans:Streams:Kafka:PartitionCount"] = "12",
                ["Orleans:Streams:Kafka:ReplicationFactor"] = "2",
                ["Orleans:Streams:Kafka:CreateTopicIfMissing"] = "false"
            })
            .Build();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProviderFromConfiguration(providerName: "kafka", configuration: configuration);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var factory = host.Services.GetServices<IQueueAdapterFactory>().OfType<KafkaQueueAdapterFactory>().Single();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.PartitionCount.Should().Be(12);
        options.ReplicationFactor.Should().Be(2);
        options.CreateTopicIfMissing.Should().BeFalse();
        factory.GetStreamQueueMapper().Should().NotBeNull();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_WhenProviderScopedSectionExists_UsesProviderScopedValues()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Orleans:Streams:Kafka:BootstrapServers"] = "legacy-host:9092",
                ["Orleans:Streams:Kafka:TopicName"] = "legacy-topic",
                ["Orleans:Streams:Kafka:kafka:BootstrapServers"] = "scoped-host:9092",
                ["Orleans:Streams:Kafka:kafka:TopicName"] = "scoped-topic",
                ["Orleans:Streams:Kafka:kafka:PartitionCount"] = "8",
                ["Orleans:Streams:Kafka:kafka:ReplicationFactor"] = "3",
                ["Orleans:Streams:Kafka:kafka:CreateTopicIfMissing"] = "true"
            })
            .Build();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProviderFromConfiguration(providerName: "kafka", configuration: configuration);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("scoped-host:9092");
        options.TopicName.Should().Be("scoped-topic");
        options.PartitionCount.Should().Be(8);
        options.ReplicationFactor.Should().Be(3);
        options.CreateTopicIfMissing.Should().BeTrue();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_WhenConnectionStringConfigured_BindsConnectionString()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Orleans:Streams:Kafka:ConnectionString"] = "bootstrap.servers=localhost:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN;sasl.username=key;sasl.password=secret",
                ["Orleans:Streams:Kafka:TopicName"] = "orders-topic",
                ["Orleans:Streams:Kafka:PartitionCount"] = "12",
                ["Orleans:Streams:Kafka:ReplicationFactor"] = "2",
                ["Orleans:Streams:Kafka:CreateTopicIfMissing"] = "false"
            })
            .Build();

        using var host = new HostBuilder()
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Guid.NewGuid().ToString("N");
                    options.ServiceId = Guid.NewGuid().ToString("N");
                });
                silo.AddKafkaStreamProviderFromConfiguration(providerName: "kafka", configuration: configuration);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.ConnectionString.Should().Contain("bootstrap.servers=localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.PartitionCount.Should().Be(12);
        options.ReplicationFactor.Should().Be(2);
        options.CreateTopicIfMissing.Should().BeFalse();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_OnClientBuilder_BindsNamedOptions()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Orleans:Streams:Kafka:BootstrapServers"] = "localhost:9092",
                ["Orleans:Streams:Kafka:TopicName"] = "orders-topic",
                ["Orleans:Streams:Kafka:PartitionCount"] = "12",
                ["Orleans:Streams:Kafka:ReplicationFactor"] = "2",
                ["Orleans:Streams:Kafka:CreateTopicIfMissing"] = "false"
            })
            .Build();

        using var host = new HostBuilder()
            .UseOrleansClient(client =>
            {
                client.AddKafkaStreamProviderFromConfiguration(providerName: "kafka", configuration: configuration);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var factory = host.Services.GetServices<IQueueAdapterFactory>().OfType<KafkaQueueAdapterFactory>().Single();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.PartitionCount.Should().Be(12);
        options.ReplicationFactor.Should().Be(2);
        options.CreateTopicIfMissing.Should().BeFalse();
        factory.GetStreamQueueMapper().Should().NotBeNull();
    }

    [TestMethod]
    public void AddKafkaStreamProviderFromConfiguration_OnClientBuilder_WhenConnectionStringConfigured_BindsConnectionString()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Orleans:Streams:Kafka:ConnectionString"] = "bootstrap.servers=localhost:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN;sasl.username=key;sasl.password=secret",
                ["Orleans:Streams:Kafka:TopicName"] = "orders-topic",
                ["Orleans:Streams:Kafka:PartitionCount"] = "12",
                ["Orleans:Streams:Kafka:ReplicationFactor"] = "2",
                ["Orleans:Streams:Kafka:CreateTopicIfMissing"] = "false"
            })
            .Build();

        using var host = new HostBuilder()
            .UseOrleansClient(client =>
            {
                client.AddKafkaStreamProviderFromConfiguration(providerName: "kafka", configuration: configuration);
            })
            .Build();

        var optionsMonitor = host.Services.GetRequiredService<IOptionsMonitor<KafkaStreamProviderOptions>>();
        var options = optionsMonitor.Get("kafka");

        options.ConnectionString.Should().Contain("bootstrap.servers=localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.PartitionCount.Should().Be(12);
        options.ReplicationFactor.Should().Be(2);
        options.CreateTopicIfMissing.Should().BeFalse();
    }
}
