#!/usr/bin/env bash
# scenario-d-latency.sh — E2E alert latencija
# Upotreba: ./scenario-d-latency.sh <broker>
# Uključuje INJECT_HIGH_TEMP=true; ingestion će ubaciti event sa
# engineTemperature=150 u INJECT_AT_S sekundi. Analytics će ispisati
# ALERT, a mi poredimo vremena između publish-a i alert-a.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

BROKER="${1:-mqtt}"
RUN_ID="scenario-D-${BROKER}-$(date +%Y%m%d-%H%M%S)"
OUT="$RESULTS_DIR/raw/$RUN_ID"
mkdir -p "$OUT"

echo "=== Scenario D: $BROKER / E2E alert latency ==="
echo "Output: $OUT"

export BROKER
export NUM_DEVICES=10
export MODE=realtime          # realtime replay, ubrzano 100x
export TIME_SCALE=100
export DURATION_S=180
export INJECT_HIGH_TEMP=true
export INJECT_AT_S=30         # na 30. sekundi ubaci kritičnu vrednost
if [ "$BROKER" = "mqtt" ]; then
    export MQTT_QOS=1
    export DB_ENABLED=false   # nema DB, samo latencija
else
    export KAFKA_ACKS=all
    export DB_ENABLED=false
fi

# KLJUČNO: shell `export` NE utiče na `docker compose --env-file`. Persistuj
# u .env PRE prvog `up` (vidi objašnjenje u scenario-a-throughput.sh). Ovde
# je posebno bitno jer INJECT_HIGH_TEMP i INJECT_AT_S nemaju default u
# compose.yaml — bez ovoga bi ingestion radio sa INJECT_HIGH_TEMP=false
# iz starog .env i scenario D ne bi proizveo nikakav ALERT.
persist_env BROKER NUM_DEVICES MODE TIME_SCALE DURATION_S \
    INJECT_HIGH_TEMP INJECT_AT_S MQTT_QOS KAFKA_ACKS DB_ENABLED

DC_BROKER="$(dc_for "$BROKER")"
$DC_BROKER down -v >/dev/null 2>&1 || true
# Ne startuj ingestion ovde: env vrednosti koje su gore exportovane bi bile
# ignorisane (compose učitava .env u vreme `up`, ne naknadno). Da bismo
# ingestion pokrenuli sa TAČNIM env vrednostima (INJECT_AT_S, INJECT_HIGH_TEMP, ...),
# prvo dižemo sve osim nje.
if [ "$BROKER" = "mqtt" ]; then
    $DC_BROKER up -d --build postgres mosquitto storage analytics >/dev/null 2>&1 || true
else
    $DC_BROKER up -d --build postgres kafka storage analytics >/dev/null 2>&1 || true
fi

wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }
if [ "$BROKER" = "kafka" ]; then
    ensure_kafka_topic || true
fi

# Pokreni docker stats collector — obavezan za §5 (CPU/RAM u tabeli)
start_metrics_collector "$OUT" 1

INJECT_TS=$(date -u +%s)
echo "[1/3] Ingestion start at $INJECT_TS; expecting INJECT at +${INJECT_AT_S}s"
$DC_BROKER up -d --force-recreate ingestion >/dev/null 2>&1 || true

# Sačeka da ingestion emit + analytics obradi
sleep $((DURATION_S - 5))

# Zaustavi metrics collector
stop_metrics_collector "$OUT"

# Sačuvaj logove i scrape-uj metrike (storage, analytics, ingest)
save_logs "$OUT"
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9090/metrics" > "$OUT/analytics_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9091/metrics" > "$OUT/ingest_metrics.txt" 2>/dev/null || true

# E2E latency = wall clock from injection to first ALERT in the injection window.
#
# Pristup:
#  1. Identifikuj ALERT liniju koja sadrži injected event (max_engine_temp=1500)
#  2. Iz nje izvuci window_end (wall clock u ms kad se prozor zatvorio)
#  3. Iz ingestion.log parsiraj [HH:MM:SS] vreme INJECT_HIGH_TEMP log-a (wall clock)
#  4. E2E = (window_end_ms/1000) - inject_unix_seconds
#
# Ovo je tačna "critical value → ALERT" latencija jer tEmit injected eventa
# postavlja scheduler na wall clock (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
# pa je window_end_ms upravo wall clock vreme zatvaranja prozora.

INJECT_LINE=$(grep "INJECT_HIGH_TEMP" "$OUT/ingestion.log" 2>/dev/null | head -1 || true)
# Izvuci HH:MM:SS iz "[HH:MM:SS WRN] INJECT_HIGH_TEMP: ..."
INJECT_HHMMSS=$(echo "$INJECT_LINE" | sed -nE 's/^\[([0-9:]+) [^]]+\].*/\1/p' || true)

# Nađi ALERT sa max_engine_temp=1500 (injekcioni prozor). window_start je u
# analitici u ms, pa je window_end_ms = window_start + 10000.
ALERT_WINDOW_END_MS=$(grep "ALERT window_start=" "$OUT/analytics.log" 2>/dev/null \
    | grep "max_engine_temp=1500" \
    | head -1 \
    | grep -oE "window_start=[0-9]+" \
    | grep -oE "[0-9]+" \
    | awk '{print $1 + 10000}' \
    || true)

if [ -n "$INJECT_HHMMSS" ] && [ -n "$ALERT_WINDOW_END_MS" ]; then
    INJECT_UNIX=$(date -u -d "today $INJECT_HHMMSS" +%s 2>/dev/null || echo 0)
    ALERT_UNIX=$((ALERT_WINDOW_END_MS / 1000))
    E2E_LATENCY=$((ALERT_UNIX - INJECT_UNIX))
    echo "E2E latency (inject→alert): ${E2E_LATENCY}s" | tee "$OUT/timing.log"
else
    # Fallback: koristi prvi ALERT timestamp - INJECT_TS
    ALERT_TS=$(grep -oE "ALERT window_start=[0-9]+" "$OUT/analytics.log" 2>/dev/null | head -1 | grep -oE "[0-9]+" || true)
    if [ -n "$ALERT_TS" ]; then
        ALERT_TS_SEC=$((ALERT_TS / 1000))
        E2E_LATENCY=$((ALERT_TS_SEC - INJECT_TS))
        echo "E2E latency (approx, fallback): ${E2E_LATENCY}s" | tee "$OUT/timing.log"
    else
        echo "No ALERT found" | tee "$OUT/timing.log"
    fi
fi

$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario D završen. Rezultati: $OUT ==="
