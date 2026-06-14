#!/usr/bin/env bash
# common.sh — zajedničke funkcije za sve scenario skripte.
# Source-ovati sa: source "$(dirname "$0")/lib/common.sh"

set -euo pipefail

# --- putanje ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_DIR="$ROOT_DIR/compose"
RESULTS_DIR="$ROOT_DIR/results"

# --- docker compose komanda ---
# --env-file .env je neophodan: docker compose v2 NE učitava .env iz
# project root-a (traži ga u direktorijumu PRVOG compose fajla, tj. compose/).
# Bez ovoga STORAGE_METRICS_PORT i ostali env ostaju na compose default-ima.
DC="docker compose --env-file $ROOT_DIR/.env -f $COMPOSE_DIR/compose.yaml"

# Odabir compose fajla po brokeru. Koriste ga scenario skripte za
# definisanje sopstvenog $DC_BROKER varijable.
compose_for() {
    local broker="$1"
    case "$broker" in
        mqtt)  echo "-f $COMPOSE_DIR/compose.mqtt.yaml" ;;
        kafka) echo "-f $COMPOSE_DIR/compose.kafka.yaml" ;;
        *)     echo "Unknown broker: $broker" >&2; return 1 ;;
    esac
}

# Vrati punu docker compose komandu za dati broker (uključujući --env-file).
dc_for() {
    local broker="$1"
    local overlay
    overlay=$(compose_for "$broker") || return 1
    echo "docker compose --env-file $ROOT_DIR/.env -f $COMPOSE_DIR/compose.yaml $overlay"
}

# Sačeka da broker bude spreman (healthcheck).
wait_for_broker() {
    local broker="$1"
    local tries=30
    if [ "$broker" = "mqtt" ]; then
        while [ $tries -gt 0 ]; do
            if docker exec "${COMPOSE_PROJECT_NAME:-iots-proj2}-mosquitto" \
                mosquitto_pub -h localhost -t health -m ok -q 0 2>/dev/null; then
                return 0
            fi
            sleep 2
            tries=$((tries - 1))
        done
    else
        while [ $tries -gt 0 ]; do
            if docker exec "${COMPOSE_PROJECT_NAME:-iots-proj2}-kafka" \
                /opt/kafka/bin/kafka-broker-api-versions.sh \
                --bootstrap-server localhost:9092 >/dev/null 2>&1; then
                return 0
            fi
            sleep 2
            tries=$((tries - 1))
        done
    fi
    echo "Broker $broker not ready" >&2
    return 1
}

# Persistuj env vrednosti u .env tako da ih `docker compose --env-file` vidi.
#
# Pozadina: `export X=Y` u shell-u NE utiče na `docker compose` jer compose
# čita .env fajl u vreme `up`, a shell exports su mu nevidljivi. Bez ovoga,
# scenario skripte koje menjaju RATE, DURATION_S, NUM_DEVICES, INJECT_*, itd.
# menjaju samo shell — kontejner se startuje sa starim vrednostima iz .env.
# Rezultat: ingestion/storage/analytics rade sa "wrong broker / wrong rate"
# iz prethodnog run-a (vidi scenario-B-kafka-20260614-182818, scenario-C-mqtt
# -20260614-182832, itd.).
#
# Upotreba (poziva se posle `export` bloka):
#   persist_env BROKER RATE DURATION_S NUM_DEVICES INJECT_HIGH_TEMP
#
# Skripta menja IN-PLACE $ROOT_DIR/.env: prvo briše sve linije koje počinju
# sa zadatim ključem (sa ili bez komentara, sa `export ` prefiksom ili bez),
# pa append-uje nove vrednosti. Sve ostale env varijable u .env (npr.
# STORAGE_METRICS_PORT, DATABASE_URL) ostaju netaknute.
persist_env() {
    local env_file="$ROOT_DIR/.env"
    for key in "$@"; do
        # Preskoči prazne vrednosti (export X= bi inače napisao "X=" u .env)
        local val="${!key:-}"
        [ -z "$val" ] && continue
        # Ukloni postojeću liniju (sa ili bez komentara, sa `export` ili bez)
        sed -i.bak -E "/^[[:space:]]*(#[[:space:]]*)?(export[[:space:]]+)?${key}=/d" "$env_file" 2>/dev/null || true
        rm -f "${env_file}.bak"
        # Append-uj novu vrednost
        printf "%s=%s\n" "$key" "$val" >> "$env_file"
    done
}

# Kreiraj Kafka topic ako ne postoji. Koristi se posle `wait_for_broker kafka`
# jer compose.kafka.yaml-ov `command:` kreira topic sa `sleep 5` racom —
# healthcheck (`kafka-broker-api-versions.sh`) može proći pre nego što se
# topic kreira, pa storage/ingestion krenu i dobiju "This server does not
# host this topic-partition" (vidi scenario-A-kafka-100-acksall).
#
# Ovo je retries-safety net: koristi `--if-not-exists` pa je idempotentan,
# i ima retry u slučaju da broker nije sasvim spreman.
ensure_kafka_topic() {
    local topic="${KAFKA_TOPIC:-iot-telemetry}"
    local partitions="${KAFKA_TOPIC_PARTITIONS:-4}"
    local rf="${KAFKA_TOPIC_RF:-1}"
    local container="${COMPOSE_PROJECT_NAME:-iots-proj2}-kafka"
    local tries=10
    while [ $tries -gt 0 ]; do
        if docker exec "$container" \
            /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 \
            --create --if-not-exists \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$rf" 2>/dev/null; then
            echo "Kafka topic '$topic' OK (partitions=$partitions, rf=$rf)"
            return 0
        fi
        sleep 2
        tries=$((tries - 1))
    done
    echo "WARN: ensure_kafka_topic failed for '$topic' (broker still not ready?)" >&2
    return 1
}

# Pokreni docker stats u pozadini i snimaj u CSV.
start_metrics_collector() {
    local outdir="$1"
    local interval="${2:-1}"
    mkdir -p "$outdir"
    local stats_csv="$outdir/stats.csv"
    # Header je neophodan — make-report-tables.py koristi csv.DictReader
    # i tretira prvi red kao zaglavlje. Bez ovoga se per-container CPU/RAM
    # izgube pa tabela prikazuje 0/0.
    if [ ! -f "$stats_csv" ] || [ ! -s "$stats_csv" ]; then
        echo "Name,CPUPerc,MemUsage,NetIO,BlockIO" > "$stats_csv"
    fi
    (
        while true; do
            docker stats --no-stream --format \
                "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}}" \
                >> "$stats_csv" 2>/dev/null || true
            sleep "$interval"
        done
    ) &
    METRICS_PID=$!
    echo "$METRICS_PID" > "$outdir/metrics.pid"
    echo "Metrics collector PID=$METRICS_PID, output=$stats_csv"
}

stop_metrics_collector() {
    local outdir="$1"
    if [ -f "$outdir/metrics.pid" ]; then
        local pid
        pid=$(cat "$outdir/metrics.pid")
        kill "$pid" 2>/dev/null || true
        rm -f "$outdir/metrics.pid"
    fi
}

# Sačuvaj ingestion log i storage log za kasniju analizu.
save_logs() {
    local outdir="$1"
    docker logs "${COMPOSE_PROJECT_NAME:-iots-proj2}-ingestion" > "$outdir/ingestion.log" 2>&1 || true
    docker logs "${COMPOSE_PROJECT_NAME:-iots-proj2}-storage"    > "$outdir/storage.log"    2>&1 || true
    docker logs "${COMPOSE_PROJECT_NAME:-iots-proj2}-analytics"  > "$outdir/analytics.log"  2>&1 || true
}

# Parsiraj ingestion log za t_emit vrednosti i izračunaj throughput.
parse_throughput() {
    local logfile="$1"
    local window_s="${2:-1}"
    # Pojednostavljeno: broji redove koji izgledaju kao publish uspeh
    # (u našem slučaju Serilog "Ingestion završen" sa emitted)
    grep -E "Ingestion završen|emitted=" "$logfile" 2>/dev/null | tail -3 || true
}

# Pokreni scenario sa cleanup-om na kraju.
with_scenario() {
    local broker="$1"
    local overlay
    overlay=$(compose_for "$broker")
    local cmd="$DC $overlay"
    echo "Command: $cmd"
    eval "$cmd"
}
