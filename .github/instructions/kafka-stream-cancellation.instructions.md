---
description: 'Review and validate cancellation in the Kafka Orleans stream receiver'
applyTo: '**/KafkaQueueAdapterReceiver.cs,**/KafkaQueueAdapterReceiverTests.cs,**/KafkaStreamProviderIntegrationTests.cs'
---

# Kafka Receiver Cancellation

When changing `KafkaQueueAdapterReceiver` or its tests, treat cancellation as an offset-safety concern, not only as an exception-handling concern.

## Required Invariants

- Check cancellation before polling, after every Kafka poll, and immediately before returning a batch.
- Record every consumed offset before checking cancellation so a canceled read can rewind the consumer position.
- Restore uncommitted offsets before allowing shutdown or disposal to invalidate the active consumer.
- Treat offset-restoration failure as fatal: invalidate and dispose the consumer, then require reinitialization.
- Check cancellation before returning from delivery, including when the receiver is uninitialized.
- Keep legacy receiver overloads as `CancellationToken.None` compatibility wrappers.

## Required Tests

Cover both already-canceled calls and cancellation after Kafka has advanced the local position. Verify that canceled reads and deliveries do not commit or lose messages, and that the next read replays the uncommitted batch.

Prefer deterministic consumer seams or fakes for timing-sensitive cases. Do not rely on short `CancelAfter` windows as the only proof of post-consumption cancellation behavior.

## Validation

Run the focused receiver tests first:

```powershell
dotnet test test/Orleans.Streams.Confluent.Tests/Orleans.Streams.Confluent.Tests.csproj --no-restore --filter FullyQualifiedName~KafkaQueueAdapterReceiverTests
```

Then run the solution build and complete test suite:

```powershell
dotnet build Orleans.Streams.Confluent.slnx --no-restore
dotnet test Orleans.Streams.Confluent.slnx --no-build --no-restore
```

Review the final diff for cancellation/shutdown interleavings before committing.
