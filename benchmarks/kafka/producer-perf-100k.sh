#!/usr/bin/env bash
# producer-perf-100k.sh — koristi Kafka producer-perf-test.sh
# Upotreba: ./producer-perf-100k.sh <num_records> <record_size>

NUM_RECORDS="${1:-100000}"
RECORD_SIZE="${2:-256}"
BROKER="${KAFKA_BROKER:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-iot-telemetry}"

echo "=== Kafka producer-perf: $NUM_RECORDS records, ${RECORD_SIZE}B ==="
docker run --rm \
    -e KAFKA_CFG_BOOTSTRAP_SERVERS="$BROKER" \
    apache/kafka:3.7.0 \
    /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "$TOPIC" \
    --num-records "$NUM_RECORDS" \
    --record-size "$RECORD_SIZE" \
    --throughput -1 \
    --producer-props bootstrap.servers="$BROKER" acks=all
