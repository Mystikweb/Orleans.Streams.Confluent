# Orleans.Streams.Confluent

Kafka stream provider for Microsoft Orleans, with optional Aspire integration packages.

## What This Library Gives You

- Kafka-backed Orleans stream provider for publish/subscribe workloads.
- Topic provisioning support (`CreateTopicIfMissing`) with partition validation.
- Flexible Kafka client configuration via direct options or connection string.
- Aspire integration for both runtime config binding and AppHost resource modeling.

## Install

Core runtime package:

```bash
dotnet add package Orleans.Streams.Confluent
```

Aspire runtime adapter package (use in Orleans server/client projects):

```bash
dotnet add package Orleans.Streams.Confluent.Aspire
```

Aspire AppHost modeling package (use in AppHost projects):

```bash
dotnet add package Orleans.Streams.Confluent.Aspire.Hosting
```

## Choose the Right Package

- Orleans silo/client project without Aspire AppHost modeling:
    use `Orleans.Streams.Confluent`.
- Orleans silo/client project with configuration-driven setup:
    use `Orleans.Streams.Confluent.Aspire`.
- Aspire AppHost project that models Orleans resources:
    use `Orleans.Streams.Confluent.Aspire.Hosting`.

## Packages

- `Orleans.Streams.Confluent`:
  Core runtime stream provider for Orleans silo/client projects.
- `Orleans.Streams.Confluent.Aspire`:
  Runtime configuration-binding helpers for Aspire-hosted Orleans projects.
- `Orleans.Streams.Confluent.Aspire.Hosting`:
  AppHost modeling extensions for `builder.AddOrleans("default")`.

## Runtime Library

Use the core runtime package when configuring Orleans directly in your silo project.

```csharp
using Orleans.Streams.Confluent;

silo.AddKafkaStreamProvider(
    providerName: "kafka",
    configureOptions: options =>
    {
        options.BootstrapServers = "localhost:9092";
        options.TopicName = "orders-topic";
        options.PartitionCount = 12;
        options.ReplicationFactor = 1;
        options.CreateTopicIfMissing = true;
    });
```

### Runtime Options

- `ConnectionString` (optional)
- `BootstrapServers`
- `TopicName`
- `PartitionCount`
- `ReplicationFactor`
- `CreateTopicIfMissing`

`TopicName` is required.

Either `BootstrapServers` or a `ConnectionString` containing `bootstrap.servers` must be provided.

If both are configured, `BootstrapServers` takes precedence over `bootstrap.servers` from `ConnectionString`.

## Connection String Configuration

`ConnectionString` is a semicolon-delimited key/value list for Kafka client settings and supports arbitrary Confluent client properties.

Example:

```text
bootstrap.servers=host1:9092,host2:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN;sasl.username=key;sasl.password=secret
```

This lets developers add security and transport settings without expanding extension method parameters.

## Aspire Integration

### Runtime Adapter (`Orleans.Streams.Confluent.Aspire`)

Use this package in Orleans server/client projects to bind provider settings from configuration.

Silo usage:

```csharp
using Orleans.Streams.Confluent.Aspire;

silo.AddKafkaStreamProviderFromConfiguration(
    providerName: "kafka",
    configuration: hostContext.Configuration,
    sectionPath: "Orleans:Streams:Kafka");
```

Client usage:

```csharp
using Orleans.Streams.Confluent.Aspire;

hostBuilder.UseOrleansClient(client =>
{
    client.AddKafkaStreamProviderFromConfiguration(
        providerName: "kafka",
        configuration: hostContext.Configuration);
});
```

The adapter prefers provider-scoped configuration (`Orleans:Streams:Kafka:{providerName}`) and falls back to `Orleans:Streams:Kafka`.

### AppHost Modeling (`Orleans.Streams.Confluent.Aspire.Hosting`)

Use this package in the AppHost project to model Kafka stream provider settings on the Orleans resource.

Bootstrap servers example:

```csharp
using Orleans.Streams.Confluent.Aspire.Hosting;

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

Connection string example:

```csharp
var orleans = builder
    .AddOrleans("default")
    .WithKafkaStreamProviderConnectionString(
        providerName: "kafka",
        connectionString: "bootstrap.servers=host1:9092,host2:9092;security.protocol=SASL_SSL;sasl.mechanism=PLAIN;sasl.username=key;sasl.password=secret",
        topicName: "orders-topic");
```

Kafka resource wiring example:

```csharp
using Aspire.Hosting;
using Orleans.Streams.Confluent.Aspire.Hosting;

var kafka = builder.AddKafka("kafka");

var orleans = builder
    .AddOrleans("default")
    .WithKafkaStreamProvider(
        providerName: "kafka",
        kafkaResource: kafka,
        topicName: "orders-topic");
```

Existing connection-string resource example:

```csharp
using Aspire.Hosting;
using Orleans.Streams.Confluent.Aspire.Hosting;

var confluent = builder.AddConnectionString("confluent-bootstrap");

var orleans = builder
    .AddOrleans("default")
    .WithKafkaStreamProvider(
        providerName: "kafka",
        kafkaResource: confluent,
        topicName: "orders-topic");
```

AppHost emits provider-scoped keys under:

- `Orleans:Streams:Kafka:{providerName}:ConnectionString`
- `Orleans:Streams:Kafka:{providerName}:BootstrapServers`
- `Orleans:Streams:Kafka:{providerName}:TopicName`
- `Orleans:Streams:Kafka:{providerName}:PartitionCount`
- `Orleans:Streams:Kafka:{providerName}:ReplicationFactor`
- `Orleans:Streams:Kafka:{providerName}:CreateTopicIfMissing`
