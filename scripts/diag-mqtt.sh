#!/usr/bin/env bash
# Diag: publish N=1000 with sequential id, count what one subscriber gets.
# Uses mosquitto_pub/sub from inside the mosquitto container.
# Run: bash scripts/diag-mqtt.sh
# Expected: received=1000, unique=1000, duplicates=0

set -euo pipefail

CONTAINER="${COMPOSE_PROJECT_NAME:-iots-proj2}-mosquitto"
N=1000
TOPIC="diag/test/device_1"
OUT_HOST="/tmp/diag-sub-$$.log"
OUT_CONT="/tmp/diag-sub.log"

# 1. Start subscriber in background. -q 0 QoS 0 (no redelivery), capture all.
docker exec "$CONTAINER" sh -c "mosquitto_sub -h localhost -t '$TOPIC' -q 0 > '$OUT_CONT' 2>/dev/null" &
SUB_PID=$!
sleep 1   # give subscription time to establish

# 2. Publish N messages with id=0..N-1 in JSON body
echo "Publishing $N messages..."
START=$(date +%s%N)
for i in $(seq 0 $((N-1))); do
    docker exec "$CONTAINER" mosquitto_pub -h localhost -t "$TOPIC" -q 0 -m "{\"id\":$i}"
done
END=$(date +%s%N)
echo "Published in $(( (END - START) / 1000000 ))ms"

# 3. Give subscriber time to drain
sleep 2
kill $SUB_PID 2>/dev/null || true
wait $SUB_PID 2>/dev/null || true

# Copy subscriber file out of container
docker cp "$CONTAINER:$OUT_CONT" "$OUT_HOST" 2>/dev/null || {
    echo "ERROR: subscriber file not produced"
    echo "container file content:"
    docker exec "$CONTAINER" cat "$OUT_CONT" 2>&1 || true
    exit 1
}
OUT="$OUT_HOST"

# 4. Analyze output
echo "--- REPORT ---"
echo "published:    $N"
TOTAL=$(wc -l < "$OUT")
echo "received:     $TOTAL"

# Extract id values
IDS=$(awk -F'"id":' '{print $2}' "$OUT" | awk -F'[,}]' '{print $1}' | sort -n)
UNIQUE=$(echo "$IDS" | uniq | wc -l)
DUPES=$((TOTAL - UNIQUE))
echo "unique ids:   $UNIQUE"
echo "duplicates:   $DUPES"

if [ "$DUPES" -gt 0 ]; then
    echo "first 5 duplicated ids:"
    echo "$IDS" | uniq -d | head -5
    echo ""
    echo "first 10 received: $(echo "$IDS" | head -10 | tr '\n' ' ')"
    echo "last 10 received:  $(echo "$IDS" | tail -10 | tr '\n' ' ')"
else
    echo "all ids unique -- no broker duplication"
fi

rm -f "$OUT"
