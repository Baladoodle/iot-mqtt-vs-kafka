#!/usr/bin/env bash
# emqtt-bench-throughput.sh — sirov throughput test sa emqtt-bench
# Upotreba: ./emqtt-bench-throughput.sh <num_connections> <msg_size> <duration_s>

set -euo pipefail

CONNECTIONS="${1:-100}"
MSG_SIZE="${2:-256}"
DURATION="${3:-30}"
HOST="${MQTT_HOST:-localhost}"
PORT="${MQTT_PORT:-1883}"

echo "=== emqtt-bench: $CONNECTIONS connections, ${MSG_SIZE}B, ${DURATION}s ==="
docker run --rm --network host \
    emqx/emqtt-bench:latest \
    pub \
    -h "$HOST" -p "$PORT" \
    -c "$CONNECTIONS" \
    -I 10 \
    -t "iot/bench/throughput" \
    -s "$MSG_SIZE" \
    -L "$DURATION"
