using System.Text;
using System.Text.Json;
using IoTAnalytics.Metrics;
using IoTAnalytics.Windows;
using MQTTnet;
using MQTTnet.Client;
// MQTTnet 5.x: IManagedMqttClient je u MQTTnet (core), ne u zasebnom paketu.

namespace IoTAnalytics.Brokers;

/// <summary>
/// MQTT consumer za Analytics. Čita sa istog topic-prefix-a kao
/// Storage, ali u svom consumer identitetu (nema shared subscription
/// za MQTT v5 uvek, pa koristimo clean=false sa unique client id).
/// </summary>
public sealed class MqttConsumer : BackgroundService
{
    private readonly ILogger<MqttConsumer> _logger;
    private readonly IManagedMqttClient _client;
    private readonly TumblingWindow _window;
    private readonly string _topic;
    private readonly int _qos;

    public MqttConsumer(ILogger<MqttConsumer> logger, TumblingWindow window, string url, string topic, int qos)
    {
        _logger = logger;
        _window = window;
        _topic = topic;
        _qos = Math.Clamp(qos, 0, 2);

        var factory = new MqttFactory();
        _client = factory.CreateManagedMqttClient();
        _client.ApplicationMessageReceivedAsync += OnMessage;

        var options = new ManagedMqttClientOptionsBuilder()
            .WithClientOptions(new MqttClientOptionsBuilder()
                .WithClientId($"analytics-{Guid.NewGuid():N}")
                .WithTcpServer(url, 1883)
                .WithCleanSession(false)
                .WithSessionExpiryInterval(0xFFFFFFFF)
                .Build())
            .Build();

        _client.StartAsync(options).GetAwaiter().GetResult();
        _client.SubscribeAsync(new MqttTopicFilterBuilder()
            .WithTopic(topic)
            .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)_qos)
            .Build()).GetAwaiter().GetResult();

        _logger.LogInformation("MQTT subscribed: topic={Topic} qos={Qos}", topic, _qos);
    }

    private Task OnMessage(MqttApplicationMessageReceivedEventArgs e)
    {
        try
        {
            var json = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            long tEmit = root.GetProperty("t_emit").GetInt64();
            float engineTemp = root.TryGetProperty("engineTemperature", out var et) ? et.GetSingle() : 0f;
            float tyreTemp = root.TryGetProperty("tyresSurfaceTemperature", out var tt) ? tt.GetSingle() : 0f;
            _window.Add(tEmit, engineTemp, tyreTemp);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("MQTT message parse failed: {Err}", ex.Message);
        }
        return Task.CompletedTask;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Delay(Timeout.Infinite, stoppingToken);

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _client.StopAsync();
        _window.Flush();
        await base.StopAsync(cancellationToken);
    }
}
