using Prometheus;

namespace IoTAnalytics.Metrics;

/// <summary>
/// Prometheus metrike za Analytics. /metrics endpoint.
/// </summary>
public static class AnalyticsMetrics
{
    // Kvalifikovano `Prometheus.Metrics` — unutar ovog namespace-a bi C# u
    // suprotnom razrešio `Metrics` kao `IoTAnalytics.Metrics`.
    public static readonly Counter MessagesTotal = Prometheus.Metrics
        .CreateCounter("analytics_messages_total", "Broj primljenih evenata od brokera.");

    public static readonly Counter AlertsTotal = Prometheus.Metrics
        .CreateCounter("analytics_alerts_total", "Broj ispisanih alert-a (mean > 50).");

    public static readonly Gauge WindowMean = Prometheus.Metrics
        .CreateGauge("analytics_window_mean_engine_temp", "Mean engine temperature u poslednjem zatvorenom prozoru.");

    public static readonly Counter LateDrops = Prometheus.Metrics
        .CreateCounter("analytics_late_drops_total", "Broj evenata odbačenih jer su van prozora.");

    public static readonly Histogram E2ELatency = Prometheus.Metrics
        .CreateHistogram("analytics_e2e_latency_seconds",
            "End-to-end latencija od publish do alert log-a (s).",
            new HistogramConfiguration
            {
                Buckets = new[] { 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30 }
            });
}
