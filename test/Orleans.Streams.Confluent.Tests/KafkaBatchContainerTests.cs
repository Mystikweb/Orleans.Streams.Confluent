using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Providers.Streams.Common;

namespace Orleans.Streams.Confluent.Tests;

[TestClass]
public sealed class KafkaBatchContainerTests
{
    [TestMethod]
    public void GetEvents_WhenRequestedTypeMatches_ReturnsTypedEventsWithPerEventTokens()
    {
        var streamId = StreamId.Create("orders", "order-123");
        var container = new KafkaBatchContainer(
            streamId,
            ["created", 42, "paid"],
            [],
            "orders-topic",
            1,
            25);

        var events = container.GetEvents<string>().ToArray();

        events.Should().HaveCount(2);
        events[0].Item1.Should().Be("created");
        events[1].Item1.Should().Be("paid");
        events[0].Item2.Should().BeOfType<EventSequenceTokenV2>();
        events[1].Item2.Should().BeOfType<EventSequenceTokenV2>();
        events[0].Item2.Should().NotBe(events[1].Item2);
    }

    [TestMethod]
    public void ImportRequestContext_WhenContextIsEmpty_ReturnsFalse()
    {
        var container = new KafkaBatchContainer(
            StreamId.Create("orders", "order-123"),
            ["created"],
            [],
            "orders-topic",
            1,
            25);

        var imported = container.ImportRequestContext();

        imported.Should().BeFalse();
    }

    [TestMethod]
    public void ImportRequestContext_WhenContextHasValues_ReturnsTrue()
    {
        var container = new KafkaBatchContainer(
            StreamId.Create("orders", "order-123"),
            ["created"],
            new Dictionary<string, object> { ["tenant"] = "acme" },
            "orders-topic",
            1,
            25);

        var imported = container.ImportRequestContext();

        imported.Should().BeTrue();
    }

    [TestMethod]
    public void ToString_WhenContainerCreated_IncludesTopicPartitionOffsetAndItemCount()
    {
        var container = new KafkaBatchContainer(
            StreamId.Create("orders", "order-123"),
            ["created", "paid"],
            [],
            "orders-topic",
            3,
            25);

        var text = container.ToString();

        text.Should().Contain("Topic=orders-topic");
        text.Should().Contain("Partition=3");
        text.Should().Contain("Offset=25");
        text.Should().Contain("#Items=2");
    }

    [TestMethod]
    public void WithKafkaMetadata_WhenApplied_UsesConsumedTopicPartitionAndOffsetForSequenceToken()
    {
        var requestContext = new Dictionary<string, object> { ["tenant"] = "acme" };
        var original = new KafkaBatchContainer(
            StreamId.Create("orders", "order-123"),
            ["created"],
            requestContext,
            "sender-topic",
            1,
            10);

        var updated = original.WithKafkaMetadata("broker-topic", 7, 42);

        updated.Topic.Should().Be("broker-topic");
        updated.Partition.Should().Be(7);
        updated.Offset.Should().Be(42);
        updated.SequenceToken.Should().BeOfType<EventSequenceTokenV2>();
        ((EventSequenceTokenV2)updated.SequenceToken).SequenceNumber.Should().Be(42);
        updated.Events.Should().Equal("created");
        updated.Events.Should().NotBeSameAs(original.Events);
        updated.RequestContext.Should().ContainKey("tenant").WhoseValue.Should().Be("acme");
        updated.RequestContext.Should().NotBeSameAs(original.RequestContext);

        original.Events.Add("paid");
        original.RequestContext["tenant"] = "contoso";

        updated.Events.Should().Equal("created");
        updated.RequestContext["tenant"].Should().Be("acme");
    }
}
