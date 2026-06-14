#!/usr/bin/env bash
# Test B (PRIMARY): Bring up the full mqtt stack (mosquitto + ingestion + storage
# + analytics) plus a 4th independent mosquitto_sub observer. Compare counts.
#
# Goal: determine whether the broker is delivering 2× to subscribers in the
# full stack, or whether storage AND analytics are coincidentally double-
# counting independently.
#
# Run: bash scripts/diag-broker-multisub.sh
# Expected total: RATE × DURATION_S = 1000 × 30 = 30,000 messages
#
# Interpretation:
#   mosquitto_sub unique t_emit = 30000  →  broker is innocent;
#                                          look for double-counting bug in
#                                          storage/analytics.
#   mosquitto_sub unique t_emit = 60000  →  broker is delivering 2× to
#                                          subscribers in this config.
#   mosquitto_sub unique t_emit ≈ 40200  →  ratio 1.34× propagates; same
#                                          puzzle but in the broker path.

set -uo pipefail   # bez -e — hocu da vidim sve greske

cd "$(dirname "$0")/.."

CONTAINER="${COMPOSE_PROJECT_NAME:-iots-proj2}-mosquitto"
DC="docker compose --env-file .env -f compose/compose.yaml -f compose/compose.mqtt.yaml"
SUB_LOG="/tmp/diag-B-sub.log"
SUB_CONT="/tmp/diag-B-sub.log"
OUT_DIR="results/raw/diag-134-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT_DIR"

# Scenario B parametara: 30k poruka, QoS 1, default clean session
# (in .env: MQTT_CLEAN_SESSION=true). Ovo odgovara originalnim 1.34× uslovima.
# MQTT_QOS može biti 0, 1 ili 2 — postavi preko env npr. MQTT_QOS_OVERRIDE=0
QOS_OVERRIDE="${MQTT_QOS_OVERRIDE:-1}"
export BROKER=mqtt
export NUM_DEVICES=100
export RATE=1000
export DURATION_S=30
export MQTT_QOS="$QOS_OVERRIDE"
export MQTT_CLEAN_SESSION=true
export DB_ENABLED=true

echo "=== TEST B: full stack + 4th mosquitto_sub observer ==="
echo "Scenario: RATE=$RATE × DURATION_S=$DURATION_S = $((RATE*DURATION_S)) expected msgs"
echo "QoS=$QOS_OVERRIDE, clean_session=$MQTT_CLEAN_SESSION"
echo

# Skini stari stack
$DC down -v >/dev/null 2>&1 || true
echo "=== Pokretanje stacka (mosquitto + storage + analytics) — ingestion NE startujemo ovde ==="
# Bez ingestion: ona će biti force-recreated niže da bi startovala sa TAČNIM
# env vrednostima koje smo upravo exportovali i da bi ingestion počela
# tek kad su storage/analytics već subscribovani. Ako bismo ovde startovali
# ingestion, ona bi publish-ovala ~12k poruka pre nego što je recreate-ujemo
# niže, a te poruke bi i dalje stigle do storage/analytics → 1.4× artifact.
$DC up -d --build postgres mosquitto storage analytics
echo "---"

# Sacekaj broker
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if docker exec "$CONTAINER" mosquitto_pub -h localhost -t health -m ok >/dev/null 2>&1; then
        echo "broker ready after ${i}s"
        break
    fi
    sleep 1
done

# Sacekaj i storage i analytics
echo "=== Čekam storage/analytics da postanu zdravi ==="
for i in 1 2 3 4 5 6 7 8 9 10 15 20 25 30; do
    S_OK=$(curl -sf -o /dev/null -w '%{http_code}' http://localhost:9093/healthz 2>/dev/null || echo "000")
    A_OK=$(curl -sf -o /dev/null -w '%{http_code}' http://localhost:9090/healthz 2>/dev/null || echo "000")
    if [ "$S_OK" = "200" ] && [ "$A_OK" = "200" ]; then
        echo "storage + analytics ready after ${i}s"
        break
    fi
    sleep 1
done

# Ocisti stari subscriber fajl ako ga ima
docker exec "$CONTAINER" rm -f "$SUB_CONT"

# Pokreni 4. observer-a (mosquitto_sub) u pozadini
echo "=== Pokretanje 4th observer: mosquitto_sub na 'iot/telemetry/#' ==="
docker exec "$CONTAINER" sh -c "mosquitto_sub -h localhost -t 'iot/telemetry/#' -q 0 > '$SUB_CONT' 2>/dev/null" &
SUB_PID=$!

# Pokreni i $SYS observer (broker-ovo internals)
SYS_LOG="/tmp/diag-B-sys.log"
SYS_CONT="/tmp/diag-B-sys.log"
docker exec "$CONTAINER" sh -c "mosquitto_sub -h localhost -t '\$SYS/broker/messages/#' -q 0 > '$SYS_CONT' 2>/dev/null" &
SYS_PID=$!
sleep 1
echo "observer PID=$SUB_PID, sys PID=$SYS_PID"

# Pokreni ingestion (force-recreate, da se ne koristi kesirani kontejner)
echo "=== Pokretanje ingestion (RATE=$RATE, DURATION_S=$DURATION_S) ==="
START_TS=$(date +%s)
$DC up -d --force-recreate ingestion
echo "ingestion start: $(date -d @$START_TS)"

# Sacekraj ingestion da zavrsi (DURATION_S + 5s grace)
# Ingestion kontejner ostaje ziv 5s posle schedulera, pa je prozor za scrape
# uzak. Delimo sleep na dve polovine i hvatamo metriku u sredini.
HALF_S=$((DURATION_S / 2 + 2))
SECOND_HALF_S=$((DURATION_S - DURATION_S / 2 + 3))
echo "=== Čekam ${HALF_S}s (1. polovina), pa cu hvatati metriku ==="
sleep "$HALF_S"

# Sredina run-a: uhvati ingestion metriku
INGEST_METRICS_MID=$(curl -sf http://localhost:9091/metrics 2>/dev/null || echo "(curl failed)")
EMITTED_MID=$(echo "$INGEST_METRICS_MID" | grep -E '^ingest_emitted_total ' | awk '{print $2}')
echo "ingestion emitovao (mid-run, ${HALF_S}s): ${EMITTED_MID:-N/A}"

echo "=== Čekam jos ${SECOND_HALF_S}s da ingestion zavrsi ==="
sleep "$SECOND_HALF_S"

# Pokusaj uhvatiti ingestion metriku odmah (pre nego sto se kontejner ugasi)
INGEST_METRICS_EARLY=$(curl -sf http://localhost:9091/metrics 2>/dev/null || echo "(curl failed)")
EMITTED_EARLY=$(echo "$INGEST_METRICS_EARLY" | grep -E '^ingest_emitted_total ' | awk '{print $2}')
echo "ingestion emitovao (pred gasenje kontejnera): ${EMITTED_EARLY:-N/A}"

# Zaustavi observer
kill $SUB_PID 2>/dev/null && echo "killed observer" || echo "observer already gone"
wait $SUB_PID 2>/dev/null
kill $SYS_PID 2>/dev/null && echo "killed sys observer" || echo "sys observer already gone"
wait $SYS_PID 2>/dev/null

# Izvuci $SYS log
docker cp "$CONTAINER:$SYS_CONT" "$SYS_LOG" 2>&1 || echo "  (docker cp SYS failed)"

# Izvuci observer log
echo "---"
echo "container observer file:"
docker exec "$CONTAINER" ls -la "$SUB_CONT" 2>&1 || echo "  (missing)"
docker exec "$CONTAINER" wc -l "$SUB_CONT" 2>&1 || true
docker cp "$CONTAINER:$SUB_CONT" "$SUB_LOG" 2>&1 || echo "  (docker cp failed)"

# Pokupi metrike od sva tri servisa
echo "---"
echo "=== METRIKE (end-of-run) ==="

# ingestion: pokusaj sada, ako nije uspelo (kontejner se ugasio), koristi EARLY
INGEST_METRICS=$(curl -sf http://localhost:9091/metrics 2>/dev/null || echo "")
if [ -z "$INGEST_METRICS" ]; then
    echo "  (ingestion container gone — using early capture)"
    INGEST_METRICS="$INGEST_METRICS_EARLY"
fi
STORAGE_METRICS=$(curl -sf http://localhost:9093/metrics 2>/dev/null || echo "(curl failed)")
ANALYTICS_METRICS=$(curl -sf http://localhost:9090/metrics 2>/dev/null || echo "(curl failed)")

EMITTED=$(echo "$INGEST_METRICS" | grep -E '^ingest_emitted_total ' | awk '{print $2}')
DROPPED=$(echo "$INGEST_METRICS" | grep -E '^ingest_dropped_total ' | awk '{print $2}')
S_RECV=$(echo "$STORAGE_METRICS" | grep -E '^storage_received_total ' | awk '{print $2}')
S_PERSIST=$(echo "$STORAGE_METRICS" | grep -E '^storage_persisted_total ' | awk '{print $2}')
A_RECV=$(echo "$ANALYTICS_METRICS" | grep -E '^analytics_messages_total ' | awk '{print $2}')

# Analiza observer loga
OBS_LINES=$(wc -l < "$SUB_LOG" 2>/dev/null || echo 0)
OBS_TOTAL_T=$(grep -oE '"t_emit":[0-9]+' "$SUB_LOG" 2>/dev/null | wc -l || echo 0)
OBS_UNIQ_T=$(grep -oE '"t_emit":[0-9]+' "$SUB_LOG" 2>/dev/null | sort -u | wc -l || echo 0)
OBS_DUPES=$((OBS_TOTAL_T - OBS_UNIQ_T))

EXPECTED=$((RATE * DURATION_S))

echo
echo "=== IZVEŠTAJ (4-kolonska komparacija) ==="
printf "%-30s %15s\n" "METRIKA" "VALUE"
printf "%-30s %15s\n" "------------------------------" "---------------"
printf "%-30s %15s\n" "expected (rate*duration)"      "$EXPECTED"
printf "%-30s %15s\n" "ingest_emitted_total"          "${EMITTED:-N/A}"
printf "%-30s %15s\n" "ingest_dropped_total"          "${DROPPED:-N/A}"
printf "%-30s %15s\n" "storage_received_total"        "${S_RECV:-N/A}"
printf "%-30s %15s\n" "storage_persisted_total"       "${S_PERSIST:-N/A}"
printf "%-30s %15s\n" "analytics_messages_total"      "${A_RECV:-N/A}"
printf "%-30s %15s\n" "---"                           "---"
printf "%-30s %15s\n" "observer (sub) total lines"    "$OBS_LINES"
printf "%-30s %15s\n" "observer t_emit total"         "$OBS_TOTAL_T"
printf "%-30s %15s\n" "observer t_emit unique"        "$OBS_UNIQ_T"
printf "%-30s %15s\n" "observer duplicates"           "$OBS_DUPES"

echo
echo "=== RACIO (vs expected=$EXPECTED) ==="
if [ -n "$EMITTED" ]; then
    R=$(awk "BEGIN {printf \"%.4f\", $EMITTED / $EXPECTED}")
    printf "  ingest_emitted / expected       = %s\n" "$R"
fi
if [ -n "$S_RECV" ]; then
    R=$(awk "BEGIN {printf \"%.4f\", $S_RECV / $EXPECTED}")
    printf "  storage_received / expected     = %s\n" "$R"
fi
if [ -n "$A_RECV" ]; then
    R=$(awk "BEGIN {printf \"%.4f\", $A_RECV / $EXPECTED}")
    printf "  analytics_messages / expected   = %s\n" "$R"
fi
if [ -n "$OBS_UNIQ_T" ] && [ "$OBS_UNIQ_T" -gt 0 ]; then
    R=$(awk "BEGIN {printf \"%.4f\", $OBS_UNIQ_T / $EXPECTED}")
    printf "  observer (uniq) / expected      = %s\n" "$R"
fi

# Verdict
echo
echo "=== VERDIKT ==="
if [ -n "$OBS_UNIQ_T" ] && [ -n "$EMITTED" ] && [ "$EMITTED" -gt 0 ]; then
    OBS_RATIO=$(awk "BEGIN {printf \"%.4f\", $OBS_UNIQ_T / $EMITTED}")
    if [ "$OBS_UNIQ_T" -eq "$EXPECTED" ] 2>/dev/null; then
        echo "  observer vidi $OBS_UNIQ_T jedinstvenih (=$EXPECTED) → broker je NEVIN"
        echo "  ako storage/analytics i dalje pokazuju 1.34×, greška je u njima"
    elif [ "$OBS_UNIQ_T" -eq $((EXPECTED * 2)) ] 2>/dev/null; then
        echo "  observer vidi $OBS_UNIQ_T (2× expected) → broker ISPORUČUJE 2× SUBSCRIBER-IMA"
        echo "  i storage i analytics dobijaju duplu količinu od brokera"
    else
        echo "  observer vidi $OBS_UNIQ_T (ratio ${OBS_RATIO}× emitted) — NEOČEKIVANO"
        echo "  treba ručno istražiti observer log"
    fi
fi

# Skini stack
echo
echo "---"
echo "=== Cleanup ==="

# Pre down: sacuvaj mosquitto log za analizu
$DC logs --no-color mosquitto > "$OUT_DIR/mosquitto.log" 2>/dev/null || true
$DC logs --no-color ingestion > "$OUT_DIR/ingestion.log" 2>/dev/null || true
$DC logs --no-color storage > "$OUT_DIR/storage.log" 2>/dev/null || true
$DC logs --no-color analytics > "$OUT_DIR/analytics.log" 2>/dev/null || true

$DC down -v >/dev/null 2>&1
echo "stack down"
echo
echo "observer fajl sacuvan na: $SUB_LOG (ne brisem — treba nam za analizu)"
echo "svi logovi sacuvani u: $OUT_DIR/"
