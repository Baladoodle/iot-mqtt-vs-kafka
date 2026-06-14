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

# KLJUČNO: shell `export` NE utiče na `docker compose --env-file`. Persistuj
# u .env PRE prvog `up` (vidi objašnjenje u scenario-a-throughput.sh).
persist_env BROKER NUM_DEVICES RATE DURATION_S MODE \
    MQTT_QOS MQTT_CLEAN_SESSION KAFKA_ACKS DB_ENABLED

DC_BROKER="$(dc_for "$BROKER")"
echo "[1/5] Pokrećem stack (bez ingestion — pokrenućemo je niže sa TAČNIM env vrednostima)..."
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

echo "[4/5] RECONNECT i polling recovery..."
docker network connect "$NETWORK" "${COMPOSE_PROJECT_NAME:-iots-proj2}-ingestion" 2>&1 | tee -a "$OUT/disconnect.log" || true
RECOVERY_START=$(date +%s)

# Polling storage_persisted_total countera — recovery = prvi tick posle
# reconnect-a gde se vrednost promenila. Prethodna implementacija je samo
# spavala DURATION_S-60 sekundi i tvrdila da je recovery "60s", što NIJE
# bilo merenje.
BASELINE=$(curl -sf "http://localhost:9093/metrics" 2>/dev/null | awk '/^storage_persisted_total /{print $2}')
echo "Baseline pre-recovery: persisted=${BASELINE}" >> "$OUT/timing.log"

RECOVERY_TIME_S=""
for i in $(seq 1 60); do
    sleep 1
    CUR=$(curl -sf "http://localhost:9093/metrics" 2>/dev/null | awk '/^storage_persisted_total /{print $2}')
    if [ -n "$CUR" ] && [ -n "$BASELINE" ] && [ "$CUR" != "$BASELINE" ]; then
        RECOVERY_TIME_S=$i
        echo "Recovery detected at +${i}s after reconnect (persisted ${BASELINE} -> ${CUR})" >> "$OUT/timing.log"
        break
    fi
done
[ -z "$RECOVERY_TIME_S" ] && RECOVERY_TIME_S="timeout" && echo "No recovery within 60s" >> "$OUT/timing.log"

# Sačeka da ingestion završi prirodno. Recovery polling je već trajao
# 1-60s iznad; ovo je dodatni buffer za prikupljanje eventualnih ALERT-a
# nakon reconnect-a i za finalni scrape metrika.
sleep 30

# Zaustavi metrics collector
stop_metrics_collector "$OUT"

# Sačuvaj logove i scrape-uj metrike
save_logs "$OUT"
curl -sf "http://localhost:9093/metrics" > "$OUT/storage_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9090/metrics" > "$OUT/analytics_metrics.txt" 2>/dev/null || true
curl -sf "http://localhost:9091/metrics" > "$OUT/ingest_metrics.txt" 2>/dev/null || true

# Pokušaj detektovati ALERT nakon reconnect (best-effort; zahteva
# da scenario B emituje INJECT_HIGH_TEMP, što trenutno ne radi jer
# INJECT_HIGH_TEMP nije uključen u scenario B env. Ostaje no-op za sada.)
ALERT_AFTER_RECONNECT=$(grep "ALERT window_start" "$OUT/analytics.log" 2>/dev/null | tail -1)
echo "First ALERT after reconnect: ${ALERT_AFTER_RECONNECT:-none}" >> "$OUT/timing.log"

echo "[5/5] Čistim stack..."
$DC_BROKER down -v >/dev/null 2>&1 || true

echo "=== Scenario B završen. Rezultati: $OUT ==="
ls -la "$OUT"
