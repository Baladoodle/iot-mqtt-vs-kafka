using Prometheus;

namespace IoTIngestion.Metrics;

/// <summary>
/// Prometheus metrike za Ingestion. Kestrel endpoint na METRICS_PORT (default 9091).
/// </summary>
public static class IngestionMetrics
{
    // `Prometheus.Metrics` je kvalifikovano jer bismo unutar ovog namespace-a
    // u suprotnom razrešili `Metrics` kao `IoTIngestion.Metrics` (C# preferira
    // trenutni namespace u odnosu na `using` direktivu).
    public static readonly Counter Emitted = Prometheus.Metrics
        .CreateCounter("ingest_emitted_total", "Ukupan broj emitovanih IoT evenata.");

    public static readonly Counter Dropped = Prometheus.Metrics
        .CreateCounter("ingest_dropped_total", "Broj evenata koje broker nije prihvatio.");

    public static readonly Gauge Throughput = Prometheus.Metrics
        .CreateGauge("ingest_throughput_msg_per_sec", "Trenutni throughput.");

    public static readonly Counter BytesEmitted = Prometheus.Metrics
        .CreateCounter("ingest_bytes_total", "Ukupan broj emitovanih bajtova.");
}
