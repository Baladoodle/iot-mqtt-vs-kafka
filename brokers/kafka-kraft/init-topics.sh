#!/bin/bash
# init-topics.sh — kreira potrebne Kafka topice pri startu kontejnera
# Pokreće se jednom, posle uspešnog starta Kafke.

set -e

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:9092}"

echo "[init-topics] Čekam Kafka broker ($BOOTSTRAP)..."

# Poll za broker
for i in $(seq 1 30); do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "[init-topics] Broker spreman."
    break
  fi
  echo "[init-topics] Pokušaj $i/30..."
  sleep 2
done

echo "[init-topics] Kreiram topic: iot-telemetry (partitions=4, RF=1)"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic iot-telemetry \
  --partitions 4 \
  --replication-factor 1

echo "[init-topics] Gotovo. Aktivni topici:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
