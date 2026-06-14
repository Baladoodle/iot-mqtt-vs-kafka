#!/usr/bin/env bash
# scenario-c-burst.sh — burst 50 → 5000 msg/s, meri backlog i recovery
# Upotreba: ./scenario-c-burst.sh <broker>
# Upravlja RATE env varijablom u toku rada ingestion servisa.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

BROKER="${1:-mqtt}"
RUN_ID="scenario-C-${BROKER}-$(date +%Y%m%d-%H%M%S)"
OUT="$RESULTS_DIR/raw/$RUN_ID"
mkdir -p "$OUT"

echo "=== Scenario C: $BROKER / burst 50→5000→50 msg/s ==="
echo "Output: $OUT"

# Env
export BROKER
export NUM_DEVICES=100
export MODE=rate
export DURATION_S=180        # 60s warm + 10s burst + 60s cool + buffer
if [ "$BROKER" = "mqtt" ]; then
    export MQTT_QOS=1
    export DB_ENABLED=true    # BACKPRESSURE kroz DB
else
    export KAFKA_ACKS=all
    export DB_ENABLED=true
fi

# KLJUČNO: shell `export` NE utiče na `docker compose --env-file`. Persistuj
# u .env PRE prvog `up` (vidi objašnjenje u scenario-a-throughput.sh).
persist_env BROKER NUM_DEVICES MODE DURATION_S MQTT_QOS KAFKA_ACKS DB_ENABLED

DC_BROKER="$(dc_for "$BROKER")"
echo "[1/4] Pokrećem stack (bez ingestion — pokrenućemo je niže sa TAČNIM env vrednostima)..."
$DC_BROKER down -v >/dev/null 2>&1 || true
# Ne startuj ingestion ovde: env vrednosti koje su gore exportovane bi bile
# ignorisane (compose učitava .env u vreme `up`, ne naknadno). Da bismo
# ingestion pokrenuli sa TAČNIM env vrednostima, prvo dižemo sve osim nje.
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

# Pokreni ingestion sa RATE=50 (warm-up)
echo "[2/4] Warm-up (60s @ 50 msg/s)..."
export RATE=50
$DC_BROKER up -d --force-recreate --no-deps ingestion >/dev/null 2>&1 || true
sleep 60

# Burst: restart ingestion sa RATE=5000
echo "[3/4] BURST (10s @ 5000 msg/s)..."
export RATE=5000
BURST_START=$(date +%s)
$DC_BROKER up -d --force-recreate --no-deps ingestion >/dev/null 2>&1 || true
sleep 10
BURST_END=$(date +%s)
echo "Burst from $BURST_START to $BURST_END" | tee "$OUT/timing.log"

# Scrape metrika ODMAH posle burst-a — uhvatićemo peak backlog
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics_burst_end.txt" 2>/dev/null || true

# Cool-down: opet RATE=50, sačeka 60s da se backlog odbrusi
echo "[4/4] Cool-down (60s @ 50 msg/s)..."
export RATE=50
$DC_BROKER up -d --force-recreate --no-deps ingestion >/dev/null 2>&1 || true
COOL_START=$(date +%s)
sleep 30
# Sredina cooldown-a: još jedan scrape da vidimo recovery trend
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics_cooldown_mid.txt" 2>/dev/null || true
sleep 30
COOL_END=$(date +%s)
echo "Cool-down from $COOL_START to $COOL_END" | tee -a "$OUT/timing.log"

# Zaustavi metrics collector
stop_metrics_collector "$OUT"

# Sačuvaj logove
save_logs "$OUT"
# Finalni scrape na kraju cooldown-a (kompatibilan sa starim putanjama)
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9090/metrics" > "$OUT/analytics_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9091/metrics" > "$OUT/ingest_metrics.txt" 2>/dev/null || true

$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario C završen. Rezultati: $OUT ==="
ls -la "$OUT"
