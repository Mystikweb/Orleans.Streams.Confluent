using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Orleans.Streams.Confluent;

internal static class KafkaClientConfigurationBuilder
{
    private const string BootstrapServersKey = "bootstrap.servers";

    public static string? ResolveBootstrapServers(KafkaStreamProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            return options.BootstrapServers;
        }

        var settings = ParseConnectionString(options.ConnectionString);
        return settings.TryGetValue(BootstrapServersKey, out var bootstrapServers) ? bootstrapServers : null;
    }

    public static AdminClientConfig CreateAdminClientConfig(KafkaStreamProviderOptions options)
    {
        return new AdminClientConfig(CreateBaseSettings(options));
    }

    public static ProducerConfig CreateProducerConfig(KafkaStreamProviderOptions options)
    {
        var settings = CreateBaseSettings(options);

        // Preserve existing defaults while allowing callers to override them through the connection string.
        settings.TryAdd("acks", "all");
        settings.TryAdd("enable.idempotence", "true");
        settings.TryAdd("allow.auto.create.topics", "false");

        return new ProducerConfig(settings);
    }

    public static ConsumerConfig CreateConsumerConfig(KafkaStreamProviderOptions options, string consumerGroup)
    {
        var settings = CreateBaseSettings(options);

        settings["group.id"] = consumerGroup;
        settings.TryAdd("enable.auto.commit", "false");
        settings.TryAdd("auto.offset.reset", "latest");
        settings.TryAdd("allow.auto.create.topics", "false");

        return new ConsumerConfig(settings);
    }

    public static Dictionary<string, string> ParseConnectionString(string? connectionString)
    {
        var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return settings;
        }

        foreach (var rawPart in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separatorIndex = rawPart.IndexOf('=');
            if (separatorIndex <= 0)
            {
                throw new ArgumentException($"Kafka connection string segment '{rawPart}' is invalid. Expected key=value format.", nameof(connectionString));
            }

            var key = NormalizeKey(rawPart[..separatorIndex].Trim());
            var value = rawPart[(separatorIndex + 1)..].Trim();
            if (key.Length == 0)
            {
                throw new ArgumentException($"Kafka connection string segment '{rawPart}' has an empty key.", nameof(connectionString));
            }

            settings[key] = value;
        }

        return settings;
    }

    private static Dictionary<string, string> CreateBaseSettings(KafkaStreamProviderOptions options)
    {
        var settings = ParseConnectionString(options.ConnectionString);
        if (!string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            settings[BootstrapServersKey] = options.BootstrapServers;
        }

        return settings;
    }

    private static string NormalizeKey(string key)
    {
        if (key.Equals("BootstrapServers", StringComparison.OrdinalIgnoreCase))
        {
            return BootstrapServersKey;
        }

        if (key.Equals("SecurityProtocol", StringComparison.OrdinalIgnoreCase))
        {
            return "security.protocol";
        }

        if (key.Equals("SaslMechanism", StringComparison.OrdinalIgnoreCase))
        {
            return "sasl.mechanism";
        }

        if (key.Equals("SaslUsername", StringComparison.OrdinalIgnoreCase))
        {
            return "sasl.username";
        }

        if (key.Equals("SaslPassword", StringComparison.OrdinalIgnoreCase))
        {
            return "sasl.password";
        }

        return key;
    }
}
