#!/usr/bin/env bash
# scenario-b-disconnect.sh — 30s docker network disconnect; meri recovery
# Upotreba: ./scenario-b-disconnect.sh <broker>
#   mqtt koristi MQTT_CLEAN_SESSION=false da se poruke čuvaju dok je offline
#   kafka automatski zadržava poruke u topic-u (recovery je samo ponovno
#   uspostavljanje konekcije ingestion → broker)

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

BROKER="${1:-mqtt}"
RUN_ID="scenario-B-${BROKER}-$(date +%Y%m%d-%H%M%S)"
OUT="$RESULTS_DIR/raw/$RUN_ID"
mkdir -p "$OUT"

echo "=== Scenario B: $BROKER / network disconnect / 30s ==="
echo "Output: $OUT"

# Env: clean_session=false za MQTT, realan rate, duži duration
export BROKER
export NUM_DEVICES=100
export RATE=500
export DURATION_S=120
export MODE=rate
if [ "$BROKER" = "mqtt" ]; then
    export MQTT_QOS=1
    export MQTT_CLEAN_SESSION=false    # KLJUČNO: čuva poruke dok je offline
    export DB_ENABLED=true
else
    export KAFKA_ACKS=all
    export DB_ENABLED=true
fi

DC_BROKER="docker compose -f $COMPOSE_DIR/compose.yaml -f $COMPOSE_DIR/compose.${BROKER}.yaml"
echo "[1/5] Pokrećem stack..."
$DC_BROKER down -v >/dev/null 2>&1 || true
$DC_BROKER up -d --build >/dev/null 2>&1 || true

wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }

echo "[2/5] Pokrećem ingestion (warm-up 30s)..."
$DC_BROKER up -d --force-recreate ingestion >/dev/null 2>&1 || true
sleep 30

echo "[3/5] DISCONNECT: docker network disconnect na ingestion..."
NETWORK="${COMPOSE_PROJECT_NAME:-iots-proj2}_default"
DISCONNECT_TIME=$(date +%s)
docker network disconnect "$NETWORK" "${COMPOSE_PROJECT_NAME:-iots-proj2}-ingestion" 2>&1 | tee -a "$OUT/disconnect.log" || true
sleep 30
RECONNECT_TIME=$(date +%s)
echo "Disconnected ${DISCONNECT_TIME}, reconnect at ${RECONNECT_TIME}" | tee "$OUT/timing.log"

echo "[4/5] RECONNECT..."
docker network connect "$NETWORK" "${COMPOSE_PROJECT_NAME:-iots-proj2}-ingestion" 2>&1 | tee -a "$OUT/disconnect.log" || true
RECOVERY_START=$(date +%s)

# Sačeka da ingestion završi prirodno
sleep $((DURATION_S - 60))

RECOVERY_END=$(date +%s)
echo "Recovery detected (approx) at: $((RECOVERY_END - RECOVERY_START))s after reconnect" | tee -a "$OUT/timing.log"

# Sačuvaj logove
save_logs "$OUT"

# Meri recovery: prvi ALERT ili prvi DB insert posle reconnect-a
ALERT_AFTER_RECONNECT=$(awk -v t="$RECONNECT_TIME" '/ALERT/ {if (NR > 0) print NR, $0}' "$OUT/analytics.log" 2>/dev/null | head -1)
echo "First ALERT after reconnect: $ALERT_AFTER_RECONNECT" | tee -a "$OUT/timing.log"

echo "[5/5] Čistim stack..."
$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario B završen. Rezultati: $OUT ==="
ls -la "$OUT"
