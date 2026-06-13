using IoTIngestion.Brokers;
using IoTIngestion.Metrics;
using IoTIngestion.Payloads;
using IoTIngestion.Replay;
using Microsoft.Extensions.Logging;
using Prometheus;
using Serilog;
using Serilog.Extensions.Logging;

namespace IoTIngestion;

public class Program
{
    public static async Task Main(string[] args)
    {
        // --- Konfiguracija iz env ---
        var broker = Environment.GetEnvironmentVariable("BROKER") ?? "mqtt";
        var numDevices = int.Parse(Environment.GetEnvironmentVariable("NUM_DEVICES") ?? "100");
        var ratePerSec = int.Parse(Environment.GetEnvironmentVariable("RATE") ?? "100");
        var durationSec = int.Parse(Environment.GetEnvironmentVariable("DURATION_S") ?? "60");
        var mode = (Environment.GetEnvironmentVariable("MODE") ?? "rate").ToLowerInvariant() == "realtime"
            ? Scheduler.Mode.Realtime
            : Scheduler.Mode.Rate;
        var timeScale = double.Parse(Environment.GetEnvironmentVariable("TIME_SCALE") ?? "1.0");
        var dataPath = Environment.GetEnvironmentVariable("DATA_CSV_PATH") ?? "/data/Data.csv";
        var topicPrefix = Environment.GetEnvironmentVariable("MQTT_TOPIC_PREFIX") ?? "iot/telemetry";
        var metricsPort = int.Parse(Environment.GetEnvironmentVariable("METRICS_PORT") ?? "9091");
        var injectHighTemp = (Environment.GetEnvironmentVariable("INJECT_HIGH_TEMP") ?? "false")
            .Equals("true", StringComparison.OrdinalIgnoreCase);
        var injectAtSec = int.Parse(Environment.GetEnvironmentVariable("INJECT_AT_S") ?? "0");

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties:j}{NewLine}{Exception}")
            .CreateLogger();

        try
        {
            Log.Information("IoT Ingestion starting: broker={Broker} devices={Devices} rate={Rate}/s mode={Mode} duration={Dur}s",
                broker, numDevices, ratePerSec, mode, durationSec);

            var builder = WebApplication.CreateBuilder(args);
            builder.Host.UseSerilog();

            // Kestrel port za /metrics
            builder.WebHost.UseUrls($"http://0.0.0.0:{metricsPort}");

            var app = builder.Build();
            app.MapMetrics();   // /metrics

            // Pretvaramo Serilog u Microsoft.Extensions.Logging.ILogger da
            // publisher/reader klase (koje koriste MEL LogInformation/...) mogu
            // da koriste Serilog kao pozadinski loger.
            using var melFactory = LoggerFactory.Create(b => b.AddSerilog(Log.Logger, dispose: false));

            // --- Izbor publishera ---
            Func<TelemetryEvent, ValueTask> publisher;
            if (broker.Equals("mqtt", StringComparison.OrdinalIgnoreCase))
            {
                var mqttUrl = Environment.GetEnvironmentVariable("MQTT_URL") ?? "mosquitto";
                var mqttQos = int.Parse(Environment.GetEnvironmentVariable("MQTT_QOS") ?? "1");
                var mqttClean = (Environment.GetEnvironmentVariable("MQTT_CLEAN_SESSION") ?? "true")
                    .Equals("true", StringComparison.OrdinalIgnoreCase);
                var clientId = $"ingest-{Environment.MachineName}-{Guid.NewGuid():N}";

                var pub = new MqttPublisher(mqttUrl, clientId, mqttQos, mqttClean, topicPrefix, melFactory.CreateLogger<MqttPublisher>());
                await pub.StartAsync(CancellationToken.None);
                publisher = pub.PublishAsync;
                Log.Information("MQTT publisher aktivan: url={Url} qos={Qos} clean={Clean}", mqttUrl, mqttQos, mqttClean);
            }
            else if (broker.Equals("kafka", StringComparison.OrdinalIgnoreCase))
            {
                var brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "kafka:9092";
                var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "iot-telemetry";
                var acks = Environment.GetEnvironmentVariable("KAFKA_ACKS") ?? "all";
                var lingerMs = int.Parse(Environment.GetEnvironmentVariable("KAFKA_LINGER_MS") ?? "5");

                var pub = new KafkaPublisher(brokers, topic, acks, lingerMs, "ingestion", melFactory.CreateLogger<KafkaPublisher>());
                publisher = pub.PublishAsync;
                Log.Information("Kafka publisher aktivan: brokers={Brokers} topic={Topic} acks={Acks}", brokers, topic, acks);
            }
            else
            {
                throw new InvalidOperationException($"Unknown BROKER: {broker}");
            }

            // --- CSV reader + scheduler ---
            var csvReader = new CsvReader(dataPath, melFactory.CreateLogger<CsvReader>());
            var scheduler = new Scheduler(
                csvReader,
                mode,
                numDevices,
                ratePerSec,
                timeScale,
                durationSec * 1000L,
                injectHighTemp,
                injectAtSec,
                melFactory.CreateLogger<Scheduler>());

            // Wrap publisher da broji metriku
            Func<TelemetryEvent, ValueTask> counted = async evt =>
            {
                await publisher(evt);
                IngestionMetrics.Emitted.Inc();
            };

            // Pokreni Kestrel u pozadini
            _ = Task.Run(() => app.Run());

            // Pokreni scheduler sa CancellationToken
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSec + 5));
            try
            {
                await scheduler.RunAsync(counted, cts.Token);
            }
            catch (OperationCanceledException) { /* expected */ }

            Log.Information("Ingestion završen. emitted={Emitted} dropped={Dropped}",
                IngestionMetrics.Emitted.Value, IngestionMetrics.Dropped.Value);

            // Drži kontejner živim još malo da Kestrel odgovori na /metrics scrape
            await Task.Delay(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Ingestion failed");
            throw;
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
    }
}
