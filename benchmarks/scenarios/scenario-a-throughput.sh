#!/usr/bin/env bash
# scenario-a-throughput.sh — meri throughput i gubitak za N uređaja × QoS/acks
# Upotreba: ./scenario-a-throughput.sh <broker> <num_devices> <qos_or_acks>
#   broker = mqtt | kafka
#   num_devices = 100 | 1000 | 10000
#   qos_or_acks = 0 | 1 | 2 (za mqtt) ili 0 | 1 | all (za kafka)

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

BROKER="${1:-mqtt}"
NUM_DEVICES="${2:-100}"
LEVEL="${3:-1}"

# QoS/acks u env ime
if [ "$BROKER" = "mqtt" ]; then
    QOS_NAME="qos${LEVEL}"
else
    QOS_NAME="acks${LEVEL}"
fi

RUN_ID="scenario-A-${BROKER}-${NUM_DEVICES}-${QOS_NAME}"
OUT="$RESULTS_DIR/raw/$RUN_ID"
mkdir -p "$OUT"

echo "=== Scenario A: $BROKER / $NUM_DEVICES devices / $QOS_NAME ==="
echo "Output: $OUT"

# Konfiguracija env
export BROKER NUM_DEVICES
if [ "$BROKER" = "mqtt" ]; then
    export MQTT_QOS="$LEVEL"
    # Scenarij A koristi DB_ENABLED=false (blackhole) da broker bude usko grlo
    export DB_ENABLED="false"
else
    export KAFKA_ACKS="$LEVEL"
    export DB_ENABLED="false"
fi
export RATE="$((NUM_DEVICES * 10))"   # 10 msg/s po uređaju
export DURATION_S="30"
export MODE="rate"

# Stack down prethodni, pa up novi
DC_BROKER="docker compose -f $COMPOSE_DIR/compose.yaml -f $COMPOSE_DIR/compose.${BROKER}.yaml"
echo "[1/4] Pokrećem stack..."
$DC_BROKER down -v >/dev/null 2>&1 || true
$DC_BROKER up -d --build >/dev/null 2>&1 || true

# Sačeka broker
echo "[2/4] Čekam broker..."
wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }

# Pokreni metrics collector
echo "[3/4] Pokrećem metrics collector i ingestion..."
start_metrics_collector "$OUT" 1

# Restart ingestion sa novim env (force recreate)
$DC_BROKER up -d --force-recreate ingestion >/dev/null 2>&1 || true

# Sačeka da ingestion završi (DURATION_S + 5s buffer)
sleep $((DURATION_S + 5))

# Zaustavi metrics i sačuvaj logove
stop_metrics_collector "$OUT"
echo "[4/4] Čuvam logove i rezultate..."
save_logs "$OUT"

# Pokušaj scrape /metrics pre nego što se ingestion završi
curl -sf "http://localhost:9091/metrics" > "$OUT/ingest_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9092/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9090/metrics" > "$OUT/analytics_metrics.txt" 2>/dev/null || true

# Down stack
$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario A završen. Rezultati: $OUT ==="
ls -la "$OUT"
