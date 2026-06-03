using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Streams.Confluent;

/// <summary>
/// A Kafka-backed Orleans batch container.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="KafkaBatchContainer"/> class.
/// </remarks>
[GenerateSerializer]
[Alias("Orleans.Streams.Confluent.KafkaBatchContainer")]
public sealed class KafkaBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object> requestContext, string topic, int partition, long offset) : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; } = streamId;

    [Id(1)]
    public List<object> Events { get; } = events ?? throw new ArgumentNullException(nameof(events));

    [Id(2)]
    public Dictionary<string, object> RequestContext { get; } = requestContext ?? [];

    [Id(3)]
    public StreamSequenceToken SequenceToken { get; } = new EventSequenceTokenV2(offset);

    [Id(4)]
    public string Topic { get; } = topic ?? throw new ArgumentNullException(nameof(topic));

    [Id(5)]
    public int Partition { get; } = partition;

    [Id(6)]
    public long Offset { get; } = offset;

    /// <inheritdoc />
    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        return Events.Select((item, index) => (item, index))
            .Where(entry => entry.item is T)
            .Select(entry =>
        {
            var token = SequenceToken is EventSequenceTokenV2 eventToken
                ? eventToken.CreateSequenceTokenForEvent(entry.index)
                : SequenceToken;

            return Tuple.Create((T)entry.item, token);
        });
    }

    /// <inheritdoc />
    public bool ImportRequestContext()
    {
        if (RequestContext.Count == 0)
        {
            return false;
        }

        RequestContextExtensions.Import(RequestContext);
        return true;
    }

    /// <summary>
    /// Creates a Kafka batch payload from Orleans stream events.
    /// </summary>
    /// <param name="serializer">The serializer used to encode the batch payload.</param>
    /// <param name="streamId">The Orleans stream identifier for the batch.</param>
    /// <param name="events">The event payloads to include in the batch.</param>
    /// <param name="requestContext">The request context to carry with the batch.</param>
    /// <param name="topic">The Kafka topic associated with the batch.</param>
    /// <param name="partition">The Kafka partition associated with the batch.</param>
    /// <param name="offset">The sequence offset used for the batch token.</param>
    /// <returns>The serialized Kafka batch payload.</returns>
    public static byte[] ToPayload<T>(Serializer<KafkaBatchContainer> serializer, StreamId streamId, IEnumerable<T> events, Dictionary<string, object> requestContext, string topic, int partition, long offset)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(events);

        var container = new KafkaBatchContainer(streamId, [.. events.Cast<object>()], requestContext, topic, partition, offset);
        return serializer.SerializeToArray(container);
    }

    /// <summary>
    /// Converts the serialized payload back into a batch container.
    /// </summary>
    public static KafkaBatchContainer FromPayload(Serializer<KafkaBatchContainer> serializer, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(payload);

        return serializer.Deserialize(payload);
    }

    /// <summary>
    /// Creates a copy of this batch container with metadata from the consumed Kafka record.
    /// </summary>
    public KafkaBatchContainer WithKafkaMetadata(string topic, int partition, long offset)
    {
        return new KafkaBatchContainer(StreamId, [.. Events], new Dictionary<string, object>(RequestContext), topic, partition, offset);
    }

    /// <summary>
    /// Creates a human-readable representation of the container.
    /// </summary>
    public override string ToString() => $"[{nameof(KafkaBatchContainer)}:Topic={Topic},Partition={Partition},Offset={Offset},#Items={Events.Count}]";
}
