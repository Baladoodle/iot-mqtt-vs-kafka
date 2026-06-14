#!/usr/bin/env bash
# Diag 2: pokreni ingestion sa RATE=10, DURATION_S=10 (ocekivano 100 msg)
# i paralelno snimaj sta mosquitto_sub dobija.
#
# Run: bash scripts/diag-publisher.sh

set -uo pipefail   # bez -e — hocu da vidim sve greske

cd "$(dirname "$0")/.."

CONTAINER="${COMPOSE_PROJECT_NAME:-iots-proj2}-mosquitto"
DC="docker compose --env-file .env -f compose/compose.yaml -f compose/compose.mqtt.yaml"
SUB_LOG="/tmp/diag-pub-sub.log"
SUB_CONT="/tmp/diag-pub-sub.log"

# Mali scenario: 10 msg/s × 10s = 100 poruka
export BROKER=mqtt
export NUM_DEVICES=10
export RATE=10
export DURATION_S=10
export MQTT_QOS=0
export MQTT_CLEAN_SESSION=true
export DB_ENABLED=false

echo "=== Pokretanje stacka (mosquitto + ingestion) ==="
$DC down -v >/dev/null 2>&1 || true
$DC up -d --build
echo "---"

# Sacekaj broker
for i in 1 2 3 4 5 6 7 8 9 10; do
    if docker exec "$CONTAINER" mosquitto_pub -h localhost -t health -m ok >/dev/null 2>&1; then
        echo "broker ready after ${i}s"
        break
    fi
    sleep 1
done

# Ocisti stari subscriber fajl ako ga ima
docker exec "$CONTAINER" rm -f "$SUB_CONT"

# Pokreni subscriber u pozadini
echo "=== Pokretanje mosquitto_sub (kontrola) ==="
docker exec "$CONTAINER" sh -c "mosquitto_sub -h localhost -t 'iot/telemetry/#' -q 0 > '$SUB_CONT' 2>/dev/null" &
SUB_PID=$!
sleep 1
echo "subscriber PID=$SUB_PID, container file=$SUB_CONT"

# Pokreni ingestion
echo "=== Pokretanje ingestion (RATE=10, DURATION_S=10) ==="
$DC up -d --force-recreate ingestion
echo "---"

# Sacekraj 5s da ingestion krene, pa odmah uzmi metriku (pre nego sto kontejner izadje)
sleep 5
INGEST_METRICS=$(curl -sf http://localhost:9091/metrics 2>/dev/null || echo "(curl failed)")
echo "ingestion metrics (mid-run):"
echo "$INGEST_METRICS" | grep -E '^ingest_emitted_total ' || echo "  (not found)"

# Sacekraj ostatak da ingestion zavrsi (ukupno ~15s)
sleep 13

# Zaustavi subscriber
kill $SUB_PID 2>/dev/null && echo "killed subscriber" || echo "subscriber already gone"
wait $SUB_PID 2>/dev/null

# Izvuci subscriber log
echo "---"
echo "container subscriber file:"
docker exec "$CONTAINER" ls -la "$SUB_CONT" 2>&1 || echo "  (missing)"
docker exec "$CONTAINER" wc -l "$SUB_CONT" 2>&1 || true
docker cp "$CONTAINER:$SUB_CONT" "$SUB_LOG" 2>&1 || echo "  (docker cp failed)"

# Uzmi ingestion metrics
echo "---"
echo "ingestion metrics endpoint:"
curl -sv http://localhost:9091/metrics 2>&1 | grep -E "(ingest_emitted_total|HTTP|connect)" | head -10
INGEST_METRICS=$(curl -sf http://localhost:9091/metrics 2>/dev/null || echo "(curl failed)")

echo ""
echo "=== IZVEŠTAJ ==="
EMITTED=$(echo "$INGEST_METRICS" | grep -E '^ingest_emitted_total ' | awk '{print $2}')
RECV_SUB=$(wc -l < "$SUB_LOG" 2>/dev/null || echo 0)
IDS=$(grep -oE '"t_emit":[0-9]+' "$SUB_LOG" 2>/dev/null | wc -l || echo 0)

echo "ingestion emitovao (counter):   ${EMITTED:-N/A}"
echo "mosquitto_sub linija u fajlu:   ${RECV_SUB:-N/A}"
echo "linija sa t_emit field-om:      ${IDS:-N/A}"

if [ -n "$EMITTED" ] && [ "$EMITTED" -gt 0 ] && [ -n "$RECV_SUB" ] && [ "$RECV_SUB" -gt 0 ]; then
    RATIO=$(awk "BEGIN {printf \"%.2f\", $RECV_SUB / $EMITTED * 100}")
    echo "ratio:                          ${RATIO}%"
    if [ "$EMITTED" = "$RECV_SUB" ]; then
        echo "BROJKE SE POKLAPAJU — publisher ne duplira u ovom scenariju"
    else
        echo "BROJKE SE RAZLIKUJU — publisher DUPLIRA"
    fi
fi

# Skini stack
echo "---"
$DC down -v >/dev/null 2>&1

# Analiza jedinstvenih t_emit vrednosti
echo ""
echo "=== DUPLIKAT ANALIZA ==="
UNIQ_T=$(grep -oE '"t_emit":[0-9]+' "$SUB_LOG" | sort -u | wc -l)
TOTAL_T=$(grep -oE '"t_emit":[0-9]+' "$SUB_LOG" | wc -l)
echo "ukupno t_emit pojavljivanja: $TOTAL_T"
echo "jedinstvenih t_emit vrednosti: $UNIQ_T"
if [ "$TOTAL_T" -gt "$UNIQ_T" ]; then
    echo "BROJ DUPLIKATA: $((TOTAL_T - UNIQ_T))"
    echo "prvih 10 t_emit vrednosti:"
    grep -oE '"t_emit":[0-9]+' "$SUB_LOG" | head -10
    echo "poslednjih 5 t_emit vrednosti:"
    grep -oE '"t_emit":[0-9]+' "$SUB_LOG" | tail -5
fi
echo ""
echo "subscriber fajl sacuvan na: $SUB_LOG (ne brisem — treba nam za analizu)"
