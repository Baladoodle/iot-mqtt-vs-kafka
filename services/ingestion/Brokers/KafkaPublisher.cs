using System.Text.Json;
using Confluent.Kafka;
using IoTIngestion.Payloads;

namespace IoTIngestion.Brokers;

/// <summary>
/// Kafka publisher. Jedan producer sa svim porukama. acks se prosleđuje
/// kroz env (BROKER_KAFKA_ACKS): 0, 1, "all".
/// </summary>
public sealed class KafkaPublisher : IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;
    private readonly ILogger _logger;
    private long _published;
    private long _dropped;

    public long Published => Interlocked.Read(ref _published);
    public long Dropped => Interlocked.Read(ref _dropped);

    public KafkaPublisher(string brokers, string topic, string acks, int lingerMs, string clientId, ILogger logger)
    {
        _topic = topic;
        _logger = logger;

        var acksEnum = acks.ToLowerInvariant() switch
        {
            "0" => Acks.None,
            "1" => Acks.Leader,
            "all" => Acks.All,
            _ => Acks.All
        };

        var config = new ProducerConfig
        {
            BootstrapServers = brokers,
            ClientId = clientId,
            Acks = acksEnum,
            EnableIdempotence = acksEnum == Acks.All,
            LingerMs = lingerMs,
            BatchSize = 256 * 1024,
            CompressionType = CompressionType.None,
            MessageSendMaxRetries = acksEnum == Acks.None ? 0 : 5,
            SocketSendBufferBytes = 1024 * 1024,
            QueueBufferingMaxMessages = 200000,
            QueueBufferingMaxKbytes = 256 * 1024
        };

        _producer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) =>
                _logger.LogWarning("Kafka error: {Code} {Reason}", e.Code, e.Reason))
            .SetLogHandler((_, m) =>
            {
                if (m.Level <= SyslogLevel.Warning)
                    _logger.LogWarning("Kafka: {Msg}", m.Message);
            })
            .Build();
    }

    public async ValueTask PublishAsync(TelemetryEvent evt)
    {
        try
        {
            var json = JsonSerializer.SerializeToUtf8Bytes(evt);
            // Fire-and-forget: visok throughput, ali gubimo mogućnost da saznamo
            // o neuspelom slanju. Ako je queue pun, odbaci (Dropped++).
            _producer.Produce(_topic, new Message<string, byte[]>
            {
                Key = evt.DeviceId,
                Value = json
            }, deliveryReport =>
            {
                if (deliveryReport.Error.IsError)
                {
                    Interlocked.Increment(ref _dropped);
                }
                else
                {
                    Interlocked.Increment(ref _published);
                }
            });
        }
        catch (ProduceException<string, byte[]> ex)
        {
            Interlocked.Increment(ref _dropped);
            _logger.LogWarning("Kafka produce failed: {Err}", ex.Error.Reason);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        await ValueTask.CompletedTask;
    }
}
