namespace IoTAnalytics.Alerts;

/// <summary>
/// Alert sink — u ovom projektu to je samo ILogger (Serilog) koji piše
/// ALERT liniju. U realnom sistemu ovo bi bio webhook / PagerDuty / Slack.
/// </summary>
public sealed class AlertSink
{
    private readonly ILogger<AlertSink> _logger;

    public AlertSink(ILogger<AlertSink> logger)
    {
        _logger = logger;
    }

    public void Sink(string message)
    {
        _logger.LogWarning("ALERT {Message}", message);
    }
}
