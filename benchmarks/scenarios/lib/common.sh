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
DC="docker compose -f $COMPOSE_DIR/compose.yaml"

# Odabir compose fajla po brokeru
compose_for() {
    local broker="$1"
    case "$broker" in
        mqtt)  echo "-f $COMPOSE_DIR/compose.mqtt.yaml" ;;
        kafka) echo "-f $COMPOSE_DIR/compose.kafka.yaml" ;;
        *)     echo "Unknown broker: $broker" >&2; return 1 ;;
    esac
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
