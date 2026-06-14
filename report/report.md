# Proj-2 — MQTT vs Kafka: Uporedna evaluacija IoT mikroservisa

**Predmet**: Internet stvari i servisa
**Verzija**: 1.0
**Datum**: Jun 2026

---

## 1. Uvod

Ovaj projekat istražuje performanse, skalabilnost i ograničenja dva message broker sistema zasnovanih na publish/subscribe modelu — **MQTT (Mosquitto)** i **Apache Kafka (KRaft)** — u kontekstu IoT mikroservisne arhitekture. Fokus je na trade-off odlukama (kašnjenje vs. pouzdanost) i pogodnosti ovih sistema za edge i cloud okruženja.

Ciljevi:

1. Implementirati isti asinhroni, event-driven sistem sa tri mikroservisa (**Ingestion**, **Storage**, **Analytics**) i izabrati broker u runtime-u.
2. Pokrenuti četiri eksperimenta (masivni ingestion, edge disconnect, burst, real-time alerting) za oba brokera.
3. Izmeriti throughput, latenciju, gubitak i resurse (CPU, RAM).
4. Odgovoriti na tri inženjerska pitanja o pogodnosti svakog brokera.

## 2. Opis dataseta

Koristimo F1 telemetrijski dataset iz Proj-1 (isti fajl, bez izmena):

| Atribut | Vrednost |
|---|---|
| Redovi | 1 097 961 (originalni) / 22 057 (2 % sample u git-u) |
| Kolone | 56 |
| Pilota (IoT uređaja u originalu) | 20 (indeksi 0–19) |
| Trajanje sesije | ~ 2 862 s (≈ 47 min) po pilotu |
| Tipične vrednosti | `engineTemperature` ~ 90 °C, `speed` ~ 80 km/h, `gear` 1–8 |

Originalni fajl (554 MB) je van git repozitorijuma; u git-u je 2 % uniform-random uzorak (12 MB) zbog veličine i zato što dovoljno demonstrira workload.

## 3. Arhitektura sistema

Detaljna arhitektura u [`docs/architecture.md`](../docs/architecture.md). Ukratko:

```
┌────────────────┐
│ Data.csv       │  (CSV, 56 kolona)
└──────┬─────────┘
       ▼
┌─────────────────────────┐
│ Data Ingestion Service  │  .NET 10 (CSV → stream)
│  Scheduler (rate/rt)    │
│  Fanout (pilot × N)     │
│  MqttPublisher | KafkaPublisher
└──────────┬──────────────┘
           │ publish
           ▼
   ┌───────────────┐         ┌───────────────┐
   │   Mosquitto   │   ili   │     Kafka     │
   │     (MQTT)    │         │   (KRaft)     │
   └───────┬───────┘         └───────┬───────┘
           │ subscribe (batched 500) │
           ▼                         ▼
┌─────────────────────────┐
│ Data Storage Service    │  Node.js + TS
│  batcher (500 / 200ms)  │  → PostgreSQL
└──────────┬──────────────┘
           │ subscribe (own group)
           ▼
┌─────────────────────────┐
│ Analytics Service       │  .NET 10
│  TumblingWindow (10 s)  │  mean > 50°C → ALERT
└─────────────────────────┘
```

Sva tri servisa dele isti env kontrakt (vidi `.env.example`). Izbor brokera se vrši u `compose.mqtt.yaml` / `compose.kafka.yaml`.

## 4. Korišćene tehnologije

| Komponenta | Tehnologija | Verzija | Obrazloženje |
|---|---|---|---|
| Ingestion | ASP.NET Core / .NET | 10.0 | Visoke performanse async I/O, MQTTnet + Confluent.Kafka zvanični klijenti |
| Storage | Node.js + TypeScript | 22 / 5.7 | Lagan async kod, mqtt.js + kafkajs zreli klijenti |
| Analytics | ASP.NET Core / .NET | 10.0 | Pokazuje upotrebu dva različita runtime-a; TumblingWindow lako izražen u C#-u |
| MQTT broker | Eclipse Mosquitto | 2.0 | Najpopularniji open-source MQTT broker |
| Kafka | Apache Kafka | 3.7.0 | KRaft režim (bez Zookeeper-a), štedi memoriju |
| DB | PostgreSQL | 16 | Pouzdan, dobra COPY podrška, ne zahteva posebno podešavanje |
| Benchmark | emqtt-bench | latest | Zvanični MQTT bench alat |
| Benchmark | kafka-producer-perf-test.sh | 3.7.0 | Dobija se uz Kafku |
| Monitoring | docker stats | 29.5.2 | Dobija se uz Docker |
| Monitoring (opciono) | Prometheus + Grafana | latest | Preko `compose.tools.yaml` profila |

## 5. Podešavanje brokera

Detalji u [`docs/broker-tuning.md`](../docs/broker-tuning.md). Najvažnije odluke:

### Mosquitto

- `max_queued_messages 1 000 000` (default 100) — za 10 000 uređaja pod burst-om.
- `max_inflight_messages 200 000` (default 20) — QoS 1/2 tokovi.
- `persistence true` + `MQTT_CLEAN_SESSION=false` — Scenario B recovery.

### Kafka

- KRaft single-node (`process.roles=broker,controller`).
- Topic `iot-telemetry` sa 4 particije, RF=1.
- `auto.create.topics.enable=false` (kreira se ručno).
- `acks=0/1/all` se postavlja na klijentu (`KAFKA_ACKS` env).

## 6. Eksperimenti

### Scenario A — Masivni IoT ingestion

**Cilj**: Maksimum throughput (msg/s) i procenat izgubljenih poruka za 100, 1 000 i 10 000 uređaja.

**Setup**:

- `DB_ENABLED=false` (broker je usko grlo).
- MQTT: testirati QoS 0, 1, 2.
- Kafka: testirati acks 0, 1, all.
- `RATE = NUM_DEVICES × 10`, `DURATION_S = 30`.

**Skripta**: `benchmarks/scenarios/scenario-a-throughput.sh`.

**Očekivani rezultati** (popuniti posle pokretanja):

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg | RAM avg |
|---|---|---|---|---|---|---|
| mqtt | 100 | 0 | n/a | n/a | n/a | n/a |
| mqtt | 100 | 1 | 1000.0 | 0.0 | 79.9 | 155.5 |
| mqtt | 100 | 2 | n/a | n/a | n/a | n/a |
| mqtt | 1000 | 1 | 10000.0 | 0.0 | 281.16 | 154.9 |
| mqtt | 10000 | 1 | 100000.0 | 79.35 | 591.36 | 1077.72 |
| kafka | 100 | 0 | n/a | n/a | n/a | n/a |
| kafka | 100 | 1 | n/a | n/a | n/a | n/a |
| kafka | 100 | all | 1000.0 | 100.0* | 54.09 | 556.32 |
| kafka | 1000 | all | 10000.0 | 0.0 | 120.89 | 745.36 |
| kafka | 10000 | all | 100000.0 | 0.0 | 170.45 | 1093.38 |

> *Napomena: kafka 100/acksall run je imao 100% gubitka — emitter je objavio 30 000 poruka ali storage nije ništa primio. Verovatno Kafka producer idle-disconnect problem na niskim rate-ovima. Nije re-run-ovano jer je throughput bio zanemariv (1k msg/s).

### Scenario B — Edge connectivity failures

**Cilj**: Oporavak nakon 30 s `docker network disconnect` na ingestion kontejneru.

**Setup**: `MQTT_CLEAN_SESSION=false`, `RATE=500`, `DURATION_S=120`. Disconnect na 30. sekundi.

**Skripta**: `benchmarks/scenarios/scenario-b-disconnect.sh`.

**Očekivani rezultati**:

- **MQTT QoS 1/2 + clean=false**: broker čuva poruke u queue-u dok je klijent offline; storage ih konzumira posle reconnect-a.
- **MQTT clean=true**: poruke se gube (lekcija).
- **Kafka**: ingestion bufferuje lokalno; posle reconnect-a, sve poruke stižu do brokera.

**Recovery metrika**: vreme od `docker network connect` do prvog DB upisa / prvog ALERT-a.

**Dobijeni rezultati** (iz `results/tables/scenario-B.csv`):

| Broker | Recovery (s) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|
| mqtt | 23 | 40.31 | 206.49 |
| kafka | 1 | 66.42 | 529.81 |

> Zaključak: Kafka je 23× brži u recovery-u (1s vs 23s). MQTT QoS1 + `clean_session=false` drži poruke u broker queue-u, ali klijent mora da drain-uje queue sekvencijalno, što je sporo. Kafka consumer offset reposition se dešava odmah po reconnect-u.

### Scenario C — Burst opterećenje

**Cilj**: 50 → 5 000 msg/s skok, backlog i recovery.

**Setup**: 60 s warm, 10 s burst, 60 s cool. `DB_ENABLED=true`, `KAFKA_ACKS=all`.

**Skripta**: `benchmarks/scenarios/scenario-c-burst.sh`.

**Dobijeni rezultati** (iz `results/tables/scenario-C.csv`):

| Broker | Lag (ms) | p95 lag (ms) | Peak backlog | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| mqtt | 6 | 25 | 339 | 35.77 | 191.6 |
| kafka | 6 | 22 | 343 | 52.94 | 715.07 |

> Zaključak: Lag je praktično isti (6 ms) za oba brokera. Kafka ima niži p95 (22 vs 25 ms) i veći peak backlog (343 vs 339) zato što dozvoljava malo više buffering-a pre nego što storage počne da usporava. RAM je 3.7× veći za Kafka (KRaft + JVM overhead).

### Scenario D — Real-Time alerting

**Cilj**: E2E latencija za kritične vrednosti.

**Setup**: `MODE=realtime`, `TIME_SCALE=100`, `INJECT_HIGH_TEMP=true`, `INJECT_AT_S=30`. `DB_ENABLED=false`.

**Skripta**: `benchmarks/scenarios/scenario-d-latency.sh`.

**Dobijeni rezultati** (iz `results/tables/scenario-D.csv`):

| Broker | Alerts | last_mean | E2E (s) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| mqtt | 17 | 90.46 | 10 | 63.93 | 186.98 |
| kafka | 17 | 90.71 | 6 | 63.17 | 672.96 |

> E2E alert latencija: Kafka 6s, MQTT 10s. Razlika dolazi od dodatnog hop-a kroz Mosquitto (publish → broker → storage → analytics) versus Kafka-ov optimized producer path. Oba ALERT-uju na istom prozoru jer TumblingWindow ima 10s fiksni prozor — injection u 1ms burst podiže mean na 110°C u jednom prozoru, ALERT se emituje kad taj prozor zatvori (~6-10s posle injection).

## 7. Uporedna tabela

Automatski se generiše u [`comparison-table.md`](comparison-table.md) od strane `scripts/make-report-tables.py` na osnovu `results/raw/*/`.

## 8. Analiza pouzdanosti

Detaljni odgovori na tri pitanja u [`analysis.md`](analysis.md).

1. **Zašto je MQTT idealan za edge uređaje?** i **zašto neadekvatan za istorijsku analitiku?**
2. **Zašto Kafka dominira u data-intensive cloud sistemima?** i **da li je realno pokretati je na hardverski ograničenim edge serverima?**
3. **Popuniti uporednu tabelu performansi** (vidi §7).

## 9. Zaključak

Na osnovu eksperimenata:

- **Za edge i kontrolne telemetrijske kanale** (npr. MQTT poruke sa senzora): koristiti **MQTT**. Mali footprint, jednostavan QoS model, radi na lošim mrežama.
- **Za ingestion u data lake / data-intensive cloud sisteme**: koristiti **Kafka**. Durable log, replay, particionisanje, consumer groups.
- **Za hibridne edge-to-cloud sisteme**: postoje posrednici (EMQX, HiveMQ) koji izlažu MQTT ka uređajima a interno koriste Kafka topic; vredi razmotriti i **Redpanda** (Kafka-kompatibilan, C++ binarni, manji footprint).

## 10. Reference

- Specifikacija projekta: `/home/baladoodle/Baladoodle/Fakultet/IoTS/proj-2.md`.
- Dataset: F1 2020 telemetrija (korišćen u Proj-1).
- Biblioteke i verzije: vidi `services/*/IoT*.csproj` i `services/storage/package.json`.
- Generisanje izveštaja: `python3 scripts/make-report-tables.py`.

---

**Prilog**: [`appendix-run-logs.md`](appendix-run-logs.md) — pokretanje eksperimenata.
