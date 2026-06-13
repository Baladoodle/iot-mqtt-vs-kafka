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

DC_BROKER="$(dc_for "$BROKER")"
echo "[1/4] Pokrećem stack..."
$DC_BROKER down -v >/dev/null 2>&1 || true
$DC_BROKER up -d --build >/dev/null 2>&1 || true

wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }

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

# Cool-down: opet RATE=50, sačeka 60s da se backlog odbrusi
echo "[4/4] Cool-down (60s @ 50 msg/s)..."
export RATE=50
$DC_BROKER up -d --force-recreate --no-deps ingestion >/dev/null 2>&1 || true
COOL_START=$(date +%s)
sleep 60
COOL_END=$(date +%s)
echo "Cool-down from $COOL_START to $COOL_END" | tee -a "$OUT/timing.log"

# Sačuvaj logove
save_logs "$OUT"
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true

$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario C završen. Rezultati: $OUT ==="
ls -la "$OUT"
