# Arhitektura sistema

Ovaj dokument opisuje mikroservisnu arhitekturu Proj-2 na višem nivou apstrakcije nego što je u samom kodu.

## Pregled

Sistem se sastoji od **tri mikroservisa** i **dva brokera** koja se biraju u vreme pokretanja:

| Servis | Tehnologija | Uloga |
|---|---|---|
| Data Ingestion | ASP.NET Core / .NET 10 | Čita `Data.csv`, emulira 100 / 1 000 / 10 000 IoT uređaja, publish-uje telemetrijske evente na broker |
| Data Storage | Node.js 22 + TypeScript | Pretplaćen na broker, koalescira u batcheve od 500 ili 200 ms, upisuje u PostgreSQL |
| Analytics | ASP.NET Core / .NET 10 | Pretplaćen u svom consumer-group-u, računa Tumbling Window (10 s) i emituje ALERT kada je srednja `engineTemperature` veća od 50 °C |

| Broker | Tehnologija | Režim |
|---|---|---|
| MQTT | Eclipse Mosquitto 2.0 | Publish/subscribe sa topicima `iot/telemetry/{device_id}`, QoS 0/1/2 |
| Kafka | Apache Kafka 3.7.0 | KRaft (bez Zookeeper-a), topic `iot-telemetry` sa 4 particije, `acks=0/1/all` |

## Dijagram toka podataka

```
Data.csv (CSV)
    │
    ▼
[Ingestion Service]  ─────publish────►  ┌──────────┐     ┌──────────┐
    (CSV → event stream)                │ Mosquitto│     │  Kafka   │
                                        │  (MQTT)  │     │ (KRaft)  │
                                        └────┬─────┘     └────┬─────┘
                                             │                │
                                  subscribe (batch 500)        │
                                             ▼                ▼
                                        [Storage Service]  (Node.js + TS)
                                             │
                                             ▼
                                        ┌──────────────┐
                                        │ PostgreSQL 16│
                                        └──────────────┘

[Analytics Service]  (Tumbling Window 10 s, .NET 10)
    ▲
    └── subscribe (own consumer group) ◄── isti broker kao Storage
```

## Tok jednog eventa

1. **Ingestion** čita jedan red iz `Data.csv`.
2. `Fanout` ga replikuje za svaku instancu uređaja (replikacija 20 pilota → N uređaja).
3. `Scheduler` odlučuje kada se emituje (rate ili realtime mod).
4. Poruka se serijalizuju u JSON oblika:
   ```json
   {
     "device_id": "5-42",
     "pilot_index": 5,
     "replica": 42,
     "sessionTime": 12.34,
     "frameIdentifier": 1234,
     "speed": 87.5,
     "engineTemperature": 92.1,
     "tyresSurfaceTemperature": 84.0,
     "worldPositionX": -123.4,
     "worldPositionY": -140.2,
     "worldPositionZ": 800.5,
     "t_emit": 1718260000000
   }
   ```
5. **MQTT**: publish na `iot/telemetry/5-42` sa QoS 0/1/2.
6. **Kafka**: publish na `iot-telemetry` sa `key=5-42` i `acks=0/1/all`.
7. **Storage** (ako je `DB_ENABLED=true`) batchuje i upisuje u Postgres.
8. **Analytics** (zaseban consumer) računa prozor i ispisuje ALERT ako treba.

## Env kontrakt

Sva tri servisa čitaju iste env varijable. Kompletna lista u [`.env.example`](../.env.example).

Ključne:

- `BROKER` — `mqtt` ili `kafka`, postavlja se u `compose.mqtt.yaml` / `compose.kafka.yaml`.
- `MQTT_QOS` / `KAFKA_ACKS` — nivo garancije isporuke.
- `MQTT_CLEAN_SESSION` — `false` za Scenario B.
- `NUM_DEVICES`, `RATE`, `DURATION_S`, `MODE` — scheduler parametri.
- `DB_ENABLED` — `true` za E2E testove, `false` za throughput testove (broker je usko grlo).
- `INJECT_HIGH_TEMP` — Scenario D injekcija kritične vrednosti.

## Metrike

Svaki servis izlaže Prometheus endpoint:

| Servis | Port |
|---|---|
| Ingestion | 9091 (`/metrics`) |
| Storage | 9092 (`/metrics`) |
| Analytics | 9090 (`/metrics`) |

Ključne metrike:

- `ingest_emitted_total` — koliko je ingestion poslao
- `storage_received_total` / `storage_persisted_total` — koliko je broker isporučio / koliko je upisano u DB
- `storage_lag_ms` / `storage_p95_lag_ms` — koliko brzo DB prihvata batcheve
- `analytics_window_mean_engine_temp` — srednja vrednost u poslednjem prozoru
- `analytics_alerts_total` — koliko je alerta ispisano
- `analytics_e2e_latency_seconds` — histogram end-to-end kašnjenja
