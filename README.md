# Orleans.Streams.Confluent

Microsoft Orleans Stream Provider for Confluent / Kafka

## Thin Aspire Adapter

This repository now includes a thin adapter project at `src/Orleans.Streams.Confluent.Aspire`.

The adapter keeps Aspire-oriented configuration binding separate from the core stream provider package.

### Usage

```csharp
using Orleans.Streams.Confluent.Aspire;

silo.AddKafkaStreamProviderFromConfiguration(
    providerName: "kafka",
    configuration: hostContext.Configuration,
    sectionPath: "Orleans:Streams:Kafka");
```

Expected configuration keys:

- `BootstrapServers`
- `TopicName`
- `PartitionCount`
- `ReplicationFactor`
- `CreateTopicIfMissing`

## Aspire AppHost Orleans Resource Modeling

This repository now also includes an AppHost-facing modeling project at `src/Orleans.Streams.Confluent.Aspire.Hosting`.

It adds `WithKafkaStreamProvider(...)` extensions for the Orleans resource created by `builder.AddOrleans("default")`.

### AppHost Usage

```csharp
using Orleans.Streams.Confluent.Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var orleans = builder
    .AddOrleans("default")
    .WithKafkaStreamProvider(
        providerName: "kafka",
        bootstrapServers: "localhost:9092",
        topicName: "orders-topic",
        partitionCount: 12,
        replicationFactor: 2,
        createTopicIfMissing: false);
```

The AppHost extension emits provider-scoped Orleans configuration keys under:

- `Orleans:Streams:Kafka:{providerName}:BootstrapServers`
- `Orleans:Streams:Kafka:{providerName}:TopicName`
- `Orleans:Streams:Kafka:{providerName}:PartitionCount`
- `Orleans:Streams:Kafka:{providerName}:ReplicationFactor`
- `Orleans:Streams:Kafka:{providerName}:CreateTopicIfMissing`

The runtime extension `AddKafkaStreamProviderFromConfiguration(...)` now prefers provider-scoped configuration and falls back to legacy `Orleans:Streams:Kafka` keys when provider-scoped keys are not present.

It is available for both Orleans silo projects and Orleans client projects:

```csharp
using Orleans.Streams.Confluent.Aspire;

hostBuilder.UseOrleansClient(client =>
{
    client.AddKafkaStreamProviderFromConfiguration(
        providerName: "kafka",
        configuration: hostContext.Configuration);
});
```
