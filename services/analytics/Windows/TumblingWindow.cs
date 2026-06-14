using System.Collections.Concurrent;
using IoTAnalytics.Metrics;

namespace IoTAnalytics.Windows;

/// <summary>
/// Tumbling window fiksne veličine (WINDOW_SIZE_MS). Ključ je
/// t_emit / WINDOW_SIZE_MS, tako da svi eventi u istom vremenskom
/// prozoru završavaju u istoj grupi.
///
/// Kada prozor "zatvori" (tj. kada stigne event čiji je ključ veći od
/// prethodnog), evaluiramo sve prethodne prozore i:
///   1. izračunamo mean engineTemperature
///   2. ako mean > ALERT_THRESHOLD, šaljemo ALERT
///   3. uklanjamo prozor iz memorije (držimo max 1000 najskorijih)
///
/// Late messages (event sa t_emit manjim od trenutnog prozora) se
/// odbacuju i broje.
/// </summary>
public sealed class TumblingWindow
{
    public const long WINDOW_SIZE_MS = 10_000;       // 10 sekundi
    private const float ALERT_THRESHOLD = 50.0f;
    private const int MaxWindowsRetained = 1000;

    private sealed class WindowState
    {
        public long WindowStartMs;
        public double EngineTempSum;
        public double TyreTempSum;
        public long Count;
        public long MaxEngineTemp;
        public long MaxTyreTemp;
        public long EarliestTEmit;
        public long LatestTEmit;
    }

    private readonly ConcurrentDictionary<long, WindowState> _windows = new();
    private long _currentWindowKey = -1;
    private readonly ILogger<TumblingWindow> _logger;

    public TumblingWindow(ILogger<TumblingWindow> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Dodaje event u prozor. Ako je event iz prošlog prozora, odbacuje ga.
    /// Vraća 'true' ako je event prihvaćen.
    /// </summary>
    public bool Add(long tEmit, float engineTemp, float tyreTemp)
    {
        long key = tEmit / WINDOW_SIZE_MS;
        long current = Interlocked.Read(ref _currentWindowKey);
        if (current >= 0 && key < current)
        {
            // Late message — prozor je već zatvoren
            AnalyticsMetrics.LateDrops.Inc();
            return false;
        }

        // Ažuriraj current window key.
        // BUGFIX: originalna logika je imala uslov `oldCurrent >= 0 && key > oldCurrent`
        // u oba grana, što znači da se _currentWindowKey NIKADA nije
        // inicijalizovao na ključ prvog eventa (jer je oldCurrent == -1).
        // Posledica: nijedan prozor nikad nije zatvoren, ALERT se nikad ne
        // emituje, gauge 'analytics_window_mean_engine_temp' ostaje 0.
        // Fix: u else grani (oldCurrent < 0) postavimo ključ na trenutni.
        long oldCurrent, initialCurrent;
        do
        {
            oldCurrent = Interlocked.Read(ref _currentWindowKey);
            if (oldCurrent >= 0 && key > oldCurrent)
            {
                // Novi prozor — zatvori sve prethodne
                initialCurrent = Interlocked.CompareExchange(ref _currentWindowKey, key, oldCurrent);
                if (initialCurrent == oldCurrent)
                {
                    // Zatvori stare prozore
                    CloseStaleWindows(oldCurrent, key);
                    break;
                }
            }
            else if (oldCurrent < 0)
            {
                // Prvi event ikad — inicijalizuj currentWindowKey na njegov ključ
                initialCurrent = Interlocked.CompareExchange(ref _currentWindowKey, key, oldCurrent);
                break;
            }
            else
            {
                // Isti ključ kao current — samo dodaj event
                initialCurrent = oldCurrent;
                break;
            }
        } while (true);

        // Dodaj u prozor
        var ws = _windows.GetOrAdd(key, k => new WindowState
        {
            WindowStartMs = k * WINDOW_SIZE_MS,
            EarliestTEmit = long.MaxValue,
            LatestTEmit = long.MinValue,
        });

        lock (ws)
        {
            ws.EngineTempSum += engineTemp;
            ws.TyreTempSum += tyreTemp;
            ws.Count++;
            if (engineTemp > ws.MaxEngineTemp) ws.MaxEngineTemp = (long)engineTemp;
            if (tyreTemp > ws.MaxTyreTemp) ws.MaxTyreTemp = (long)tyreTemp;
            if (tEmit < ws.EarliestTEmit) ws.EarliestTEmit = tEmit;
            if (tEmit > ws.LatestTEmit) ws.LatestTEmit = tEmit;
        }

        AnalyticsMetrics.MessagesTotal.Inc();
        return true;
    }

    private void CloseStaleWindows(long upToKey, long newKey)
    {
        var keys = _windows.Keys.Where(k => k < newKey).OrderBy(k => k).ToList();
        foreach (var k in keys)
        {
            if (_windows.TryGetValue(k, out var ws))
            {
                EvaluateWindow(ws);
                _windows.TryRemove(k, out _);
            }
        }
    }

    private void EvaluateWindow(WindowState ws)
    {
        if (ws.Count == 0) return;
        double meanEngine = ws.EngineTempSum / ws.Count;
        double meanTyre = ws.TyreTempSum / ws.Count;
        long e2eMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ws.EarliestTEmit;

        AnalyticsMetrics.WindowMean.Set(meanEngine);
        AnalyticsMetrics.E2ELatency.Observe(e2eMs / 1000.0);

        if (meanEngine > ALERT_THRESHOLD)
        {
            AnalyticsMetrics.AlertsTotal.Inc();
            // Emituj ALERT log u strogo formatiranom obliku da ga harness lako parsuje
            _logger.LogWarning(
                "ALERT window_start={WindowStart} window_end={WindowEnd} count={Count} mean_engine_temp={Mean:F2} mean_tyre_temp={TyreMean:F2} max_engine_temp={MaxEngine} max_tyre_temp={MaxTyre} e2e_latency_ms={E2EMs}",
                ws.WindowStartMs,
                ws.WindowStartMs + WINDOW_SIZE_MS,
                ws.Count,
                meanEngine,
                meanTyre,
                ws.MaxEngineTemp,
                ws.MaxTyreTemp,
                e2eMs);
        }
    }

    /// <summary>
    /// Forsiraj zatvaranje prozora (za graceful shutdown).
    /// </summary>
    public void Flush()
    {
        var keys = _windows.Keys.OrderBy(k => k).ToList();
        foreach (var k in keys)
        {
            if (_windows.TryGetValue(k, out var ws))
            {
                EvaluateWindow(ws);
                _windows.TryRemove(k, out _);
            }
        }
    }
}
