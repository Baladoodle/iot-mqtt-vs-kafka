# Proj-2 — MQTT vs Kafka: IoT Microservices Comparative Evaluation

Event-driven IoT mikroservisni sistem koji uporedno evaluira **MQTT (Mosquitto)** i **Apache Kafka (KRaft)** kao message brokere. Projekat je deo kursa *Internet stvari i servisa* (drugi projekat).

## Mikroservisna arhitektura

```
                    ┌─────────────────────┐
                    │  Data Ingestion     │  (.NET 10)
                    │  service: simulira  │  CSV → MQTT/Kafka
                    │  100..10 000 IoT    │
                    │  uređaja            │
                    └─────────┬───────────┘
                              │  publish
                              ▼
              ┌───────────────┴───────────────┐
              │                               │
       ┌──────┴──────┐                 ┌──────┴──────┐
       │  Mosquitto  │                 │   Kafka     │
       │  (MQTT)     │                 │  (KRaft)    │
       └──────┬──────┘                 └──────┬──────┘
              │  subscribe (batched 500)      │
              ▼                               ▼
                    ┌─────────────────────┐
                    │  Data Storage       │  (Node.js + TS)
                    │  service: upisuje   │  → PostgreSQL
                    │  u PostgreSQL       │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │  Analytics service  │  (.NET 10)
                    │  Tumbling Window    │  10 s, alert kada
                    │  (10 s agregacija)  │  prosek > 50 °C
                    └─────────────────────┘
```

## Brzi start

```bash
# 1. Klonirati, postaviti Data.csv (vidi "Dataset" dole)
cp .env.example .env

# 2. MQTT varijanta
docker compose -f compose/compose.yaml -f compose/compose.mqtt.yaml up -d --build

# 3. Kafka varijanta (zaustaviti MQTT prvo)
docker compose -f compose/compose.yaml -f compose/compose.mqtt.yaml down
docker compose -f compose/compose.yaml -f compose/compose.kafka.yaml up -d --build
```

Detaljne instrukcije po scenariju su u [`docs/scenarios.md`](docs/scenarios.md).

## Dataset

`Data.csv` (12 MB, ~22 000 redova, **commit-ovan**) je 2 % uniform-random uzorak
originalnog dataseta od 1 097 961 redova × 56 kolona (F1 telemetrija, 20 pilota).

Originalni `Data.full.csv` (554 MB) se čuva na disku pored `Proj-2/` i **nije**
deo git repozitorijuma (vidi `.gitignore`). Ako želite da pokrenete eksperimente
na punom datasetu, zamenite `Data.csv` originalom (vidi `docs/scenarios.md`).

```
sha256sum Data.csv        # 85a1ca1666a7720629bd5c08165dbec6f8b67004360dcdefbfca084b3d86db43
sha256sum Data.full.csv   # 4023f852d920c0c7abd0267408fd912694c5a9e8d1334d8852befc3f819a79fa
```

## Struktura projekta

| Putanja | Sadržaj |
|---|---|
| `compose/` | `compose.yaml` (baza) + per-broker overlay fajlovi |
| `brokers/` | Konfiguracije za Mosquitto i Kafka (KRaft) |
| `services/ingestion/` | .NET 10 — simulator IoT uređaja |
| `services/storage/` | Node.js + TypeScript — batched Postgres sink |
| `services/analytics/` | .NET 10 — Tumbling Window + alert |
| `postgres/init.sql` | Šema baze |
| `benchmarks/scenarios/` | Skripte za Scenario A/B/C/D |
| `scripts/` | Pomoćne skripte (reset, healthcheck, izveštaj) |
| `results/` | Sirovi i agregirani rezultati eksperimenata |
| `report/` | Tehnički izveštaj (srpski) |
| `docs/` | Arhitektura, scenariji, broker-tuning, outline izveštaja |

## Eksperimentalni scenariji

| Scenario | Šta meri | Ključne skripte |
|---|---|---|
| **A** Masivni ingestion | Throughput + gubitak poruka za 100 / 1 000 / 10 000 uređaja | `benchmarks/scenarios/scenario-a-throughput.sh` |
| **B** Edge disconnect | 30 s `docker network disconnect`; oporavak pretplate/offseta | `benchmarks/scenarios/scenario-b-disconnect.sh` |
| **C** Burst opterećenje | 50 → 5 000 msg/s skok; backlog i backpressure | `benchmarks/scenarios/scenario-c-burst.sh` |
| **D** E2E alert latencija | t_alert − t_emit za injektovanu kritičnu vrednost | `benchmarks/scenarios/scenario-d-latency.sh` |

## Tehnologije

- **Ingestion**: ASP.NET Core / .NET 10, MQTTnet, Confluent.Kafka
- **Storage**: Node.js 22 + TypeScript, mqtt.js, kafkajs, `pg` (Postgres COPY)
- **Analytics**: ASP.NET Core / .NET 10, MQTTnet, Confluent.Kafka
- **MQTT broker**: Eclipse Mosquitto 2.0
- **Kafka**: Apache Kafka 3.7 (KRaft režim, bez Zookeeper-a)
- **DB**: PostgreSQL 16
- **Benchmark alati**: emqtt-bench, kafkaproducer-perf-test.sh, k6, docker stats

## Reprodukovanje rezultata

```bash
# Pokrenuti sve scenarije za oba brokera (~2 sata)
./benchmarks/scenarios/scenario-a-throughput.sh mqtt  100 1
./benchmarks/scenarios/scenario-a-throughput.sh mqtt  1000 1
./benchmarks/scenarios/scenario-a-throughput.sh mqtt  10000 1
./benchmarks/scenarios/scenario-a-throughput.sh kafka 100 1
# ... (ostali)
python3 scripts/make-report-tables.py     # generiše results/tables/*
```

## Licence & autor

Projekat je akademski rad. Podaci potiču iz F1 2020 telemetrije (korišćeni u Proj-1).
