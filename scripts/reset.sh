#!/usr/bin/env bash
# reset.sh — potpuno čišćenje docker stack-a (down -v, briše volumes)
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
COMPOSE_DIR="$ROOT/compose"

DC="docker compose -f $COMPOSE_DIR/compose.yaml"

echo "=== Reset Proj-2 stack ==="
echo "[1/2] Brišem MQTT stack ako postoji..."
$DC -f $COMPOSE_DIR/compose.mqtt.yaml down -v 2>/dev/null || true

echo "[2/2] Brišem Kafka stack ako postoji..."
$DC -f $COMPOSE_DIR/compose.kafka.yaml down -v 2>/dev/null || true

echo "Brišem docker volumes ako postoje..."
docker volume rm iots-proj2_pgdata iots-proj2_mqtt_data iots-proj2_kafka_data 2>/dev/null || true

echo "Reset završen."
