using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;

namespace Orleans.Streams.Confluent.Tests;

[TestClass]
public sealed class KafkaStreamProviderBuilderExtensionsTests
{
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
    }
}
