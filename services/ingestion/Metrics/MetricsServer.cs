using Prometheus;

namespace IoTIngestion.Metrics;

/// <summary>
/// Prometheus metrike za Ingestion. Kestrel endpoint na METRICS_PORT (default 9091).
/// </summary>
public static class IngestionMetrics
{
    public static readonly Counter Emitted = Metrics
        .CreateCounter("ingest_emitted_total", "Ukupan broj emitovanih IoT evenata.");

    public static readonly Counter Dropped = Metrics
        .CreateCounter("ingest_dropped_total", "Broj evenata koje broker nije prihvatio.");

    public static readonly Gauge Throughput = Metrics
        .CreateGauge("ingest_throughput_msg_per_sec", "Trenutni throughput.");

    public static readonly Counter BytesEmitted = Metrics
        .CreateCounter("ingest_bytes_total", "Ukupan broj emitovanih bajtova.");
}
