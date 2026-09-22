using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Streams;

namespace Orleans.Streams.Confluent.Tests;

[TestClass]
public sealed class KafkaQueueAdapterReceiverTests
{
    [TestMethod]
    public async Task GetQueueMessagesAsync_WhenCancellationRequested_ThrowsOperationCanceledException()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var receiver = CreateReceiver();

        Func<Task> act = async () => await receiver.GetQueueMessagesAsync(10, cancellationTokenSource.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [TestMethod]
    public async Task MessagesDeliveredAsync_WhenCancellationRequested_ThrowsOperationCanceledException()
    {
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        var receiver = CreateReceiver();
        var messages = new IBatchContainer[]
        {
            new KafkaBatchContainer(StreamId.Create("orders", "order-123"), ["created"], [], "orders-topic", 0, 0)
        };

        Func<Task> act = async () => await receiver.MessagesDeliveredAsync(messages, cancellationTokenSource.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static KafkaQueueAdapterReceiver CreateReceiver()
        => new(
            "provider",
            null!,
            null!,
            NullLogger<KafkaQueueAdapterReceiver>.Instance,
            default,
            "consumer-group");
}
