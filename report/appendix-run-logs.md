# Prilog — Dnevnik pokretanja

Ovaj fajl pokazuje kako se eksperimenti pokreću i gde se nalaze rezultati. Generiše se ručno ili se čuva `git log` komandi koje su pokrenute.

## 1. Preflight

```bash
./scripts/preflight.sh
# očekivano: "Preflight OK" sa svim zelenim ✓
```

## 2. Stack up (MQTT varijanta)

```bash
cp .env.example .env
docker compose -f compose/compose.yaml -f compose/compose.mqtt.yaml up -d --build
./scripts/healthcheck.sh
```

## 3. Stack up (Kafka varijanta)

```bash
docker compose -f compose/compose.yaml -f compose/compose.mqtt.yaml down -v
docker compose -f compose/compose.yaml -f compose/compose.kafka.yaml up -d --build
./scripts/healthcheck.sh
```

## 4. Scenario A — 100 uređaja, MQTT QoS 1

```bash
./benchmarks/scenarios/scenario-a-throughput.sh mqtt 100 1
ls results/raw/scenario-A-mqtt-100-qos1/
# ingestion.log storage.log analytics.log stats.csv *_metrics.txt
```

## 5. Scenario B — MQTT disconnect

```bash
./benchmarks/scenarios/scenario-b-disconnect.sh mqtt
cat results/raw/scenario-B-mqtt-*/timing.log
```

## 6. Scenario C — Kafka burst

```bash
./benchmarks/scenarios/scenario-c-burst.sh kafka
cat results/raw/scenario-C-kafka-*/timing.log
```

## 7. Scenario D — E2E latencija

```bash
./benchmarks/scenarios/scenario-d-latency.sh mqtt
./benchmarks/scenarios/scenario-d-latency.sh kafka
grep "E2E" results/raw/scenario-D-*/timing.log
```

## 8. Generisanje izveštaja

```bash
python3 scripts/make-report-tables.py
cat report/comparison-table.md
```

## 9. Cleanup

```bash
./scripts/reset.sh
```
