using IoTAnalytics.Brokers;
using IoTAnalytics.Windows;
using Prometheus;
using Serilog;

namespace IoTAnalytics;

public class Program
{
    public static async Task Main(string[] args)
    {
        var broker = Environment.GetEnvironmentVariable("BROKER") ?? "mqtt";
        var metricsPort = int.Parse(Environment.GetEnvironmentVariable("METRICS_PORT") ?? "9090");
        var topicPrefix = Environment.GetEnvironmentVariable("MQTT_TOPIC_PREFIX") ?? "iot/telemetry/#";

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Enrich.FromLogContext()
            .WriteTo.Console(outputTemplate:
                "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} {Properties:j}{NewLine}{Exception}")
            .CreateLogger();

        try
        {
            Log.Information("IoT Analytics starting: broker={Broker}", broker);

            var builder = WebApplication.CreateBuilder(args);
            builder.Host.UseSerilog();
            builder.WebHost.UseUrls($"http://0.0.0.0:{metricsPort}");

            var app = builder.Build();
            app.MapMetrics();
            app.MapGet("/healthz", () => Results.Ok(new { status = "ok", broker }));

            // --- Singletons ---
            var window = new TumblingWindow(app.Services.GetRequiredService<ILogger<TumblingWindow>>());

            if (broker.Equals("mqtt", StringComparison.OrdinalIgnoreCase))
            {
                var url = Environment.GetEnvironmentVariable("MQTT_URL") ?? "mosquitto";
                var qos = int.Parse(Environment.GetEnvironmentVariable("MQTT_QOS") ?? "1");
                _ = new MqttConsumer(
                    app.Services.GetRequiredService<ILogger<MqttConsumer>>(),
                    window, url, topicPrefix, qos);
            }
            else if (broker.Equals("kafka", StringComparison.OrdinalIgnoreCase))
            {
                var brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "kafka:9092";
                var topic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "iot-telemetry";
                var group = Environment.GetEnvironmentVariable("KAFKA_GROUP") ?? "analytics-cg";
                _ = new KafkaConsumer(
                    app.Services.GetRequiredService<ILogger<KafkaConsumer>>(),
                    window, brokers, topic, group);
            }
            else
            {
                throw new InvalidOperationException($"Unknown BROKER: {broker}");
            }

            Log.Information("Analytics ready, listening on :{Port}/metrics", metricsPort);
            await app.RunAsync();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Analytics failed");
            throw;
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
    }
}
