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
        var factory = host.Services.GetServices<IQueueAdapterFactory>().OfType<KafkaQueueAdapterFactory>().Single();
        var options = optionsMonitor.Get("kafka");

        options.BootstrapServers.Should().Be("localhost:9092");
        options.TopicName.Should().Be("orders-topic");
        options.CreateTopicIfMissing.Should().BeFalse();
        options.PartitionCount.Should().Be(6);
        factory.GetStreamQueueMapper().Should().NotBeNull();
    }
}
