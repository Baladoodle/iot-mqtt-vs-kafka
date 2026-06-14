using System.Text.Json;
using Confluent.Kafka;
using IoTAnalytics.Windows;

namespace IoTAnalytics.Brokers;

/// <summary>
/// Kafka consumer za Analytics. Zaseban consumer group 'analytics-cg'
/// da vidi isti tok kao Storage.
/// </summary>
public sealed class KafkaConsumer : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly TumblingWindow _window;
    private readonly string _topic;
    private CancellationTokenSource? _cts;

    public KafkaConsumer(ILogger<KafkaConsumer> logger, TumblingWindow window, string brokers, string topic, string groupId)
    {
        _logger = logger;
        _window = window;
        _topic = topic;

        var config = new ConsumerConfig
        {
            BootstrapServers = brokers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5_000,
            SessionTimeoutMs = 30_000,
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogWarning("Kafka error: {Code} {Reason}", e.Code, e.Reason))
            .Build();

        _consumer.Subscribe(topic);
        _logger.LogInformation("Kafka subscribed: topic={Topic} group={Group}", topic, groupId);

        // BUGFIX: Analytics se instancira kao `_ = new KafkaConsumer(...)` u
        // Program.cs, što ga NE registruje kao IHostedService. Posledica:
        // ExecuteAsync se nikad ne pozove i Consume() loop se nikad ne pokrene.
        // Da bismo dobili isti efekat kao kod MQTT consumer-a (koji koristi
        // event subscription u konstruktoru), pokreni Consume loop odmah
        // ovde u fire-and-forget tasku. StopAsync komunicira preko _cts.
        _cts = new CancellationTokenSource();
        _ = Task.Run(() => RunConsumeLoop(_cts.Token));
    }

    private void RunConsumeLoop(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var msg = _consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (msg == null || msg.Message == null) continue;
                var payload = msg.Message.Value;
                using var doc = JsonDocument.Parse(payload);
                var root = doc.RootElement;
                long tEmit = root.GetProperty("t_emit").GetInt64();
                float engineTemp = root.TryGetProperty("engineTemperature", out var et) ? et.GetSingle() : 0f;
                float tyreTemp = root.TryGetProperty("tyresSurfaceTemperature", out var tt) ? tt.GetSingle() : 0f;
                _window.Add(tEmit, engineTemp, tyreTemp);
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning("Kafka consume error: {Err}", ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError("Kafka loop error: {Err}", ex.Message);
            }
        }
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Delay(Timeout.Infinite, stoppingToken);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts?.Cancel();
        _consumer.Close();
        _window.Flush();
        await base.StopAsync(cancellationToken);
    }
}
