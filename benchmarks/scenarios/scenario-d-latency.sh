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

DC_BROKER="$(dc_for "$BROKER")"
$DC_BROKER down -v >/dev/null 2>&1 || true
$DC_BROKER up -d --build >/dev/null 2>&1 || true

wait_for_broker "$BROKER" || { echo "Broker not ready"; exit 1; }

INJECT_TS=$(date -u +%s)
echo "[1/3] Ingestion start at $INJECT_TS; expecting INJECT at +${INJECT_AT_S}s"
$DC_BROKER up -d --force-recreate ingestion >/dev/null 2>&1 || true

# Sačeka da ingestion emit + analytics obradi
sleep $((DURATION_S - 5))

# Prikupljamo alert timestamp
ALERT_TS=$(grep -oE "ALERT window_start=[0-9]+" "$OUT/analytics.log" 2>/dev/null | head -1 | grep -oE "[0-9]+" || true)
if [ -n "$ALERT_TS" ]; then
    ALERT_TS_SEC=$((ALERT_TS / 1000))
    E2E_LATENCY=$((ALERT_TS_SEC - INJECT_TS - INJECT_AT_S))
    echo "E2E latency (approx): ${E2E_LATENCY}s" | tee "$OUT/timing.log"
else
    echo "No ALERT found" | tee "$OUT/timing.log"
fi

save_logs "$OUT"
$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario D završen. Rezultati: $OUT ==="
