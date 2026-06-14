using System.Diagnostics;
using IoTIngestion.Device;
using IoTIngestion.Payloads;
using IoTIngestion.Replay;

namespace IoTIngestion.Replay;

/// <summary>
/// Dva moda:
///   - realtime: emituje jedan red po ΔsessionTime / TIME_SCALE milisekundi.
///   - rate:     emituje po RATE poruka/s × NUM_DEVICES, ponavljajući CSV.
/// </summary>
public sealed class Scheduler
{
    public enum Mode { Realtime, Rate }

    private readonly CsvReader _reader;
    private readonly ILogger _logger;
    private readonly Mode _mode;
    private readonly int _numDevices;
    private readonly int _ratePerSec;
    private readonly double _timeScale;
    private readonly long _durationMs;
    private readonly bool _injectHighTemp;
    private readonly int _injectAtSec;

    public Scheduler(
        CsvReader reader,
        Mode mode,
        int numDevices,
        int ratePerSec,
        double timeScale,
        long durationMs,
        bool injectHighTemp,
        int injectAtSec,
        ILogger logger)
    {
        _reader = reader;
        _mode = mode;
        _numDevices = numDevices;
        _ratePerSec = ratePerSec;
        _timeScale = timeScale;
        _durationMs = durationMs;
        _injectHighTemp = injectHighTemp;
        _injectAtSec = injectAtSec;
        _logger = logger;
    }

    /// <summary>
    /// Pokreće scheduler. publishAsync se poziva za svaki event. Awaituje
    /// publisher-ov backpressure (npr. bounded queue) tako da ne pretrpamo
    /// memoriju ako je broker spor.
    /// </summary>
    public async Task RunAsync(
        Func<TelemetryEvent, ValueTask> publishAsync,
        CancellationToken ct)
    {
        var devices = Fanout.BuildDevices(_numDevices).ToList();
        _logger.LogInformation(
            "Scheduler start: mode={Mode} devices={Devices} rate={Rate}/s timeScale={Scale} duration={DurMs}ms",
            _mode, _numDevices, _ratePerSec, _timeScale, _durationMs);

        if (_mode == Mode.Rate)
        {
            await RunRateAsync(devices, publishAsync, ct).ConfigureAwait(false);
        }
        else
        {
            await RunRealtimeAsync(devices, publishAsync, ct).ConfigureAwait(false);
        }
    }

    private async Task RunRateAsync(
        IReadOnlyList<Fanout.VirtualDevice> devices,
        Func<TelemetryEvent, ValueTask> publishAsync,
        CancellationToken ct)
    {
        // Ukupno poruka: rate × duration_s.
        // Ako je to više nego redova u CSV-u, loopujemo CSV.
        long totalToEmit = (long)(_ratePerSec * (_durationMs / 1000.0));
        if (totalToEmit < 1) totalToEmit = 1;

        long emitted = 0;
        var sw2 = Stopwatch.StartNew();
        bool injected = false;

        // Učitaj sve redove u memoriju (rate mode ne zavisi od sessionTime)
        var rows = new List<CsvReader.CsvRow>();
        foreach (var row in _reader.Read())
        {
            rows.Add(row);
        }
        _logger.LogInformation("Učitano {Count} redova iz CSV-a", rows.Count);

        // Vremenski razmak između poruka za održavanje RATE
        long intervalMicros = 1_000_000 / _ratePerSec;  // razmak između poruka
        int rowIdx = 0;

        try
        {
            while (emitted < totalToEmit && !ct.IsCancellationRequested)
            {
                var row = rows[rowIdx % rows.Count];
                rowIdx++;
                // emitted je long; IReadOnlyList indexer prima int.
                var device = devices[(int)(emitted % devices.Count)];

                // INJECT_HIGH_TEMP: u rate modu se aktivira posle _injectAtSec sekundi
                // wall-clock vremena schedulera. Inject-uje 100 eventa sa
                // engineTemperature=1500°C u jednoj sekundi (isti device, isti
                // TEmit okvir) da bi analytics-ov 10s prozor značajno podigao
                // mean i ispalio ALERT. Za A/B/C se ne koristi.
                if (_injectHighTemp && !injected && sw2.Elapsed.TotalSeconds >= _injectAtSec)
                {
                    injected = true;
                    _logger.LogWarning(
                        "INJECT_HIGH_TEMP: emitting 100 critical events (engineTemp=1500) at +{Sec:F1}s of replay time (target +{Target}s) device={DeviceId}",
                        sw2.Elapsed.TotalSeconds, _injectAtSec, device.DeviceId);
                    var injectTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    for (int i = 0; i < 100; i++)
                    {
                        var critical = BuildPayload(row, device);
                        critical.EngineTemperature = 1500.0f;
                        critical.TEmit = injectTime + i;  // 1ms razmaka, svi u istom 10s prozoru
                        await publishAsync(critical).ConfigureAwait(false);
                    }
                }

                var payload = BuildPayload(row, device);
                await publishAsync(payload).ConfigureAwait(false);
                emitted++;

                // Ograničenje brzine
                long targetMicros = emitted * intervalMicros;
                long actualMicros = sw2.ElapsedTicks * 1_000_000 / Stopwatch.Frequency;
                long sleepMicros = targetMicros - actualMicros;
                if (sleepMicros > 0)
                {
                    await Task.Delay(TimeSpan.FromMicroseconds(sleepMicros), ct).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _logger.LogInformation("Rate mode završen: emitted={Emitted}/{Total}", emitted, totalToEmit);
        }
    }

    private async Task RunRealtimeAsync(
        IReadOnlyList<Fanout.VirtualDevice> devices,
        Func<TelemetryEvent, ValueTask> publishAsync,
        CancellationToken ct)
    {
        long emitted = 0;
        long lastEmitMs = -1;
        var swInject = Stopwatch.StartNew();
        bool injected = false;

        try
        {
            // Skup svih uređaja, replay po uređaju
            foreach (var device in devices)
            {
                long prevSessionMs = 0;
                foreach (var row in _reader.Read())
                {
                    if (ct.IsCancellationRequested) break;

                    long sessionMs = (long)(row.SessionTime * 1000 / _timeScale);
                    long wait = sessionMs - prevSessionMs;
                    if (wait > 0)
                    {
                        await Task.Delay((int)Math.Min(wait, 1000), ct).ConfigureAwait(false);
                    }
                    prevSessionMs = sessionMs;

                    // INJECT_HIGH_TEMP: u realtime modu aktiviramo jednom kada
                    // _injectAtSec sekundi wall-clock vremena scheduler-a
                    // protekne. Inject-ujemo 100 eventa sa engineTemperature=
                    // 1500°C u jednoj sekundi (isti device, susedni TEmit)
                    // da bi analytics-ov 10s prozor dobio dovoljno kritičnih
                    // vrednosti da mu mean pređe ALERT_THRESHOLD=50°C.
                    if (_injectHighTemp && !injected && swInject.Elapsed.TotalSeconds >= _injectAtSec)
                    {
                        injected = true;
                        _logger.LogWarning(
                            "INJECT_HIGH_TEMP: emitting 100 critical events (engineTemp=1500) at +{Sec:F1}s of replay time (target +{Target}s) device={DeviceId}",
                            swInject.Elapsed.TotalSeconds, _injectAtSec, device.DeviceId);
                        var injectTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        for (int i = 0; i < 100; i++)
                        {
                            var critical = BuildPayload(row, device);
                            critical.EngineTemperature = 1500.0f;
                            critical.TEmit = injectTime + i;  // 1ms razmaka, svi u istom 10s prozoru
                            await publishAsync(critical).ConfigureAwait(false);
                        }
                    }

                    var payload = BuildPayload(row, device);
                    await publishAsync(payload).ConfigureAwait(false);
                    emitted++;

                    if (_durationMs > 0 && lastEmitMs < 0)
                    {
                        lastEmitMs = sessionMs;
                    }
                    if (lastEmitMs > 0 && sessionMs - lastEmitMs >= _durationMs) break;
                }
            }
        }
        finally
        {
            // finally blok garantuje log i pri cancellation-u (npr. kada
            // scenario-d-latency.sh prekine ingestion pre nego što prirodno
            // završi). Bez ovoga parse_ingest_log u report generatoru ne
            // može da izvuče emitted.
            _logger.LogInformation("Realtime mode završen: emitted={Emitted}", emitted);
        }
    }

    private TelemetryEvent BuildPayload(
        CsvReader.CsvRow row,
        Fanout.VirtualDevice device)
    {
        return new TelemetryEvent
        {
            DeviceId = device.DeviceId,
            PilotIndex = device.PilotIndex,
            Replica = device.Replica,
            SessionTime = row.SessionTime,
            FrameIdentifier = row.FrameIdentifier,
            Speed = row.Speed,
            EngineTemperature = row.EngineTemperature,
            TyresSurfaceTemperature = row.TyresSurfaceTemperature,
            WorldPositionX = row.WorldPositionX,
            WorldPositionY = row.WorldPositionY,
            WorldPositionZ = row.WorldPositionZ,
            TEmit = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
    }
}
