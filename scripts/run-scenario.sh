#!/usr/bin/env bash
# run-scenario.sh — top-level entry point za pokretanje jednog scenarija.
#
# Upotreba:
#   scripts/run-scenario.sh <A|B|C|D> <mqtt|kafka> [num_devices] [level]
#
# Argumenti:
#   A            Scenario A: throughput po uređajima i QoS/ACKS
#                Zahteva: num_devices (100|1000|10000), level (qos0|1|2 | acks0|1|all)
#   B            Scenario B: 30s network disconnect, recovery
#                Koristi fiksne default-e (num_devices=100, qos=1 / acks=all).
#   C            Scenario C: burst 50 → 5000 → 50 msg/s
#                Koristi fiksne default-e (num_devices=100, qos=1 / acks=all).
#   D            Scenario D: E2E alert latencija
#                Koristi fiksne default-e (num_devices=10, qos=1 / acks=all).
#
# 'level' se prihvata i kao:
#   - numerička vrednost: 0 | 1 | 2  (mqtt)  ili  0 | 1 | all  (kafka)
#   - imenovana vrednost: qos0|qos1|qos2 | acks0|acks1|acksall
# Imenovana forma se interno normalizuje na numeričku pre poziva scenarija.
#
# Izlaz:
#   results/raw/scenario-X-broker-N-level/ — ingestion.log, storage.log,
#   analytics.log, stats.csv, *_metrics.txt, timing.log.
#
# Opcije:
#   --no-preflight    preskoči preflight.sh proveru (za CI / ponovljene run-ove)
#   --list, -l        ispiši poznate scenarije i izađi
#   -h, --help        ova pomoć

set -euo pipefail

# --- putanje (ne zavisimo od toga odakle se skripta poziva) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCENARIOS_DIR="$ROOT_DIR/benchmarks/scenarios"
cd "$ROOT_DIR"

# --- pomoćne funkcije ---
RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YEL=$'\033[0;33m'; NC=$'\033[0m'
ok()   { echo -e "${GRN}✓${NC} $1"; }
warn() { echo -e "${YEL}!${NC} $1"; }
err()  { echo -e "${RED}✗${NC} $1" >&2; }

usage() {
    sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
}

list_scenarios() {
    cat <<EOF
Poznati scenariji:

  A  Throughput  benchmarks/scenarios/scenario-a-throughput.sh
                  args: <broker> <num_devices> <level>
                  level: qos0|qos1|qos2 (mqtt) | acks0|acks1|acksall (kafka)

  B  Disconnect  benchmarks/scenarios/scenario-b-disconnect.sh
                  args: <broker>
                  fixni: num_devices=100, qos=1 (mqtt) / acks=all (kafka)

  C  Burst       benchmarks/scenarios/scenario-c-burst.sh
                  args: <broker>
                  fixni: num_devices=100, qos=1 (mqtt) / acks=all (kafka)

  D  Latency     benchmarks/scenarios/scenario-d-latency.sh
                  args: <broker>
                  fixni: num_devices=10, qos=1 (mqtt) / acks=all (kafka)

Primeri:
  scripts/run-scenario.sh A mqtt 100 qos1
  scripts/run-scenario.sh A kafka 1000 acksall
  scripts/run-scenario.sh B mqtt
  scripts/run-scenario.sh D kafka
EOF
}

# --- arg parse ---
PREFLIGHT=1
while [ $# -gt 0 ]; do
    case "$1" in
        --no-preflight) PREFLIGHT=0; shift ;;
        -l|--list)      list_scenarios; exit 0 ;;
        -h|--help)      usage; exit 0 ;;
        --)             shift; break ;;
        -*)             err "Nepoznata opcija: $1"; usage; exit 1 ;;
        *)              break ;;
    esac
done

if [ $# -lt 2 ]; then
    err "Nedovoljno argumenata."
    usage
    exit 1
fi

SCENARIO="$1"
BROKER="$2"
NUM_DEVICES="${3:-}"
LEVEL="${4:-}"

# --- validacija ---
case "$SCENARIO" in
    A|B|C|D) ;;
    *) err "Nepoznat scenario: $SCENARIO (očekivano A|B|C|D)"; exit 1 ;;
esac
case "$BROKER" in
    mqtt|kafka) ;;
    *) err "Nepoznat broker: $BROKER (očekivano mqtt|kafka)"; exit 1 ;;
esac

# --- normalizacija level-a (qos1 → 1, acksall → all, ...) ---
normalize_level() {
    local broker="$1" raw="$2"
    case "${raw,,}" in
        "")           echo ""; return 0 ;;
        qos0|acks0|0) echo "0" ;;
        qos1|acks1|1) echo "1" ;;
        qos2|2)       [ "$broker" = "mqtt" ] && echo "2" || { err "Kafka nema QoS 2 (acks=2 ne postoji)"; return 1; } ;;
        acksall|all)  [ "$broker" = "kafka" ] && echo "all" || { err "MQTT nema ack=all (koristite qos0|1|2)"; return 1; } ;;
        *) err "Nepoznat level: $raw"; return 1 ;;
    esac
}

# --- default-i po scenariju ---
case "$SCENARIO" in
    A)
        NUM_DEVICES="${NUM_DEVICES:-100}"
        if [ -z "$LEVEL" ]; then
            err "Scenario A zahteva level (qos0|qos1|qos2 | acks0|acks1|acksall)."
            exit 1
        fi
        LEVEL="$(normalize_level "$BROKER" "$LEVEL")" || exit 1
        case "$NUM_DEVICES" in
            100|1000|10000) ;;
            *) err "num_devices mora biti 100, 1000 ili 10000 (dato: $NUM_DEVICES)"; exit 1 ;;
        esac
        ;;
    B|C)
        NUM_DEVICES="${NUM_DEVICES:-100}"
        # B i C imaju fiksni level; ignoriši prosleđeni ako ga ima.
        if [ -n "$LEVEL" ]; then
            warn "Scenario $SCENARIO ignoriše prosleđeni level=$LEVEL (koristi qos=1 / acks=all)."
        fi
        LEVEL=""
        ;;
    D)
        NUM_DEVICES="${NUM_DEVICES:-10}"
        if [ -n "$LEVEL" ]; then
            warn "Scenario D ignoriše prosleđeni level=$LEVEL (koristi qos=1 / acks=all)."
        fi
        LEVEL=""
        ;;
esac

# --- preflight ---
if [ "$PREFLIGHT" = 1 ]; then
    ok "Pokrećem preflight..."
    if ! "$SCRIPT_DIR/preflight.sh"; then
        err "Preflight failed — popravi navedeno i pokušaj ponovo."
        exit 1
    fi
    echo
fi

# --- dispatch ---
case "$SCENARIO" in
    A) TARGET="$SCENARIOS_DIR/scenario-a-throughput.sh" ;;
    B) TARGET="$SCENARIOS_DIR/scenario-b-disconnect.sh" ;;
    C) TARGET="$SCENARIOS_DIR/scenario-c-burst.sh" ;;
    D) TARGET="$SCENARIOS_DIR/scenario-d-latency.sh" ;;
esac

if [ ! -x "$TARGET" ]; then
    err "Skripta ne postoji ili nije izvršna: $TARGET"
    exit 1
fi

# Lep prikaz pokretanja
ok "Scenario: $SCENARIO"
ok "Broker:   $BROKER"
[ -n "$NUM_DEVICES" ] && ok "Devices:  $NUM_DEVICES"
[ -n "$LEVEL" ] && ok "Level:    $LEVEL"
ok "Skripta:  $TARGET"
echo

# Persistuj visoko-nivo varijable u .env. Scenario skripte dodaju i ostale
# (RATE, DURATION_S, INJECT_*, ...), ali BROKER i NUM_DEVICES su ovde
# poznati i treba da budu u .env pre nego što scenario skripta pokrene
# `docker compose up`. Bez ovoga, docker compose čita .env iz prethodnog
# run-a i ingestion/storage/analytics rade sa pogrešnim brokerom (vidi
# scenario-B-kafka-20260614-182818 za konkretan primer).
ENV_FILE="$ROOT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
    for key in BROKER NUM_DEVICES; do
        if [ -n "${!key:-}" ]; then
            sed -i.bak -E "/^[[:space:]]*(#[[:space:]]*)?(export[[:space:]]+)?${key}=/d" "$ENV_FILE" 2>/dev/null || true
            rm -f "${ENV_FILE}.bak"
            printf "%s=%s\n" "$key" "${!key}" >> "$ENV_FILE"
        fi
    done
fi

# Pozovi sa pravim argumentima
if [ -n "$LEVEL" ]; then
    exec "$TARGET" "$BROKER" "$NUM_DEVICES" "$LEVEL"
else
    exec "$TARGET" "$BROKER"
fi
