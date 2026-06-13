using System.Text;
using System.Text.Json;
using IoTIngestion.Payloads;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Protocol;

namespace IoTIngestion.Brokers;

/// <summary>
/// MQTT publisher. Koristi ManagedMqttClient (MQTTnet.Extensions) jer ima
/// bolji backpressure (bounded queue + reconnect).
/// </summary>
public sealed class MqttPublisher : IAsyncDisposable
{
    private readonly IManagedMqttClient _client;
    private readonly string _topicPrefix;
    private readonly MqttQualityOfServiceLevel _qos;
    private readonly ILogger _logger;
    private readonly ManagedMqttClientOptions _options;

    public MqttPublisher(string url, string clientId, int qos, bool cleanSession, string topicPrefix, ILogger logger)
    {
        _topicPrefix = topicPrefix;
        _logger = logger;
        _qos = (MqttQualityOfServiceLevel)Math.Clamp(qos, 0, 2);

        var factory = new MqttFactory();
        _client = factory.CreateManagedMqttClient();

        _options = new ManagedMqttClientOptionsBuilder()
            .WithClientOptions(new MqttClientOptionsBuilder()
                .WithClientId(clientId)
                .WithTcpServer(url, 1883)
                .WithCleanSession(cleanSession)
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
                .WithSessionExpiryInterval(cleanSession ? 0 : 0xFFFFFFFF)  // 0 = session ends on disconnect, max = persistent
                .Build())
            .Build();
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _client.DisconnectedAsync += args =>
        {
            _logger.LogWarning("MQTT disconnected: {Reason}", args.Reason);
            return Task.CompletedTask;
        };
        _client.ConnectedAsync += args =>
        {
            _logger.LogInformation("MQTT connected");
            return Task.CompletedTask;
        };
        await _client.StartAsync(_options).ConfigureAwait(false);
    }

    public async ValueTask PublishAsync(TelemetryEvent evt)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(evt);
        var msg = new MqttApplicationMessageBuilder()
            .WithTopic($"{_topicPrefix}/{evt.DeviceId}")
            .WithPayload(json)
            .WithQualityOfServiceLevel(_qos)
            .Build();

        // Enqueue — ManagedMqttClient interno batchuje
        await _client.EnqueueAsync(msg).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.StopAsync().ConfigureAwait(false);
        _client.Dispose();
    }
}
