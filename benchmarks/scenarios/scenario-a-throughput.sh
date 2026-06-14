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

# KLJUČNO: shell `export` NE utiče na `docker compose --env-file` (compose čita
# .env u vreme `up`). Bez ovoga, ingestion bi radio sa starim .env vrednostima
# iz prethodnog scenarija → "wrong broker / wrong rate" simptomi. Persistuj
# SVE scenario-specific varijable u .env PRE prvog `up`.
persist_env BROKER NUM_DEVICES RATE DURATION_S MODE MQTT_QOS KAFKA_ACKS DB_ENABLED

# Stack down prethodni, pa up novi
DC_BROKER="$(dc_for "$BROKER")"
echo "[1/4] Pokrećem stack (bez ingestion — pokrenućemo je niže sa TAČNIM env vrednostima)..."
$DC_BROKER down -v >/dev/null 2>&1 || true
# Ne startuj ingestion ovde: env vrednosti koje smo gore exportovali bi bile
# ignorisane (compose učitava .env u vreme `up`, ne naknadno). Ako bismo ovde
# startovali ingestion sa .env default-ima, pa je recreate-ovali niže sa
# ovim env, prva instanca bi već publish-ovala ~12k poruka → 1.4× artifact.
if [ "$BROKER" = "mqtt" ]; then
    $DC_BROKER up -d --build postgres mosquitto storage analytics >/dev/null 2>&1 || true
else
    $DC_BROKER up -d --build postgres kafka storage analytics >/dev/null 2>&1 || true
fi

# Sačeka broker
echo "[2/4] Čekam broker..."
wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }
# Za kafka, osiguraj da je topic kreiran (compose.kafka.yaml-ov init može
# kasniti za healthcheck-om → storage fatalno puca sa "topic-partition
# doesn't exist").
if [ "$BROKER" = "kafka" ]; then
    ensure_kafka_topic || true
fi

# Pokreni metrics collector
echo "[3/4] Pokrećem metrics collector i ingestion..."
start_metrics_collector "$OUT" 1

# Start ingestion sa novim env (force recreate — osiguravamo da nema
# starih kontejnera i da se pokrene sa svežim env).
$DC_BROKER up -d --force-recreate ingestion >/dev/null 2>&1 || true

# Sačeka ingestion završi. Ingestion drži kontejner živim još 5s posle
# scheduler-a (Program.cs Task.Delay 5s) da Kestrel odgovori na /metrics
# scrape. Spavamo DURATION_S + 2 da scrape padne sredinom tog prozora —
# nikako DURATION_S + 5+ (tada je ingestion već izašao).
sleep $((DURATION_S + 2))

# Zaustavi metrics i sačuvaj logove
stop_metrics_collector "$OUT"
echo "[4/4] Čuvam logove i rezultate..."
save_logs "$OUT"

# Pokušaj scrape /metrics pre nego što se ingestion završi
curl -sf "http://localhost:9091/metrics" > "$OUT/ingest_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9090/metrics" > "$OUT/analytics_metrics.txt" 2>/dev/null || true

# Down stack
$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario A završen. Rezultati: $OUT ==="
ls -la "$OUT"
