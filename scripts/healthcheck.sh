#!/usr/bin/env bash
# healthcheck.sh — proverava da li su broker + 3 servisa živi i /metrics
# rade. Pozvati posle `docker compose up -d` sa strpljenjem.

set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

GREEN="\033[0;32m"
RED="\033[0;31m"
YEL="\033[0;33m"
NC="\033[0m"

check() {
    local name="$1" url="$2"
    if curl -sf "$url" >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name ($url)"
    else
        echo -e "  ${RED}✗${NC} $name ($url)"
    fi
}

echo "=== Proj-2 Healthcheck ==="

# Brokere prepoznajemo po tome koji kontejneri su pokrenuti.
if docker ps --format '{{.Names}}' | grep -q "mosquitto"; then
    echo "MQTT varijanta:"
    check "Mosquitto TCP" "http://localhost:1883" || true   # biće fail jer je MQTT, ali označava
    if docker exec iots-proj2-mosquitto mosquitto_pub -h localhost -t health -m ok -q 0 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} Mosquitto pub/sub"
    else
        echo -e "  ${RED}✗${NC} Mosquitto pub/sub"
    fi
fi

if docker ps --format '{{.Names}}' | grep -q "kafka"; then
    echo "Kafka varijanta:"
    if docker exec iots-proj2-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} Kafka broker API"
    else
        echo -e "  ${RED}✗${NC} Kafka broker API"
    fi
fi

echo "Servisi:"
check "Ingestion /metrics" "http://localhost:9091/metrics"
check "Storage /metrics"   "http://localhost:9092/metrics"
check "Analytics /metrics" "http://localhost:9090/metrics"
check "Postgres"           "http://localhost:5432" || true
