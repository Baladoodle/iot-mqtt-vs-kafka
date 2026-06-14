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

**Dobijeni rezultati** (iz `results/tables/scenario-A.csv`, poslednji validan run):

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU ukupni | RAM ukupni |
|---|---|---|---|---|---|---|
| mqtt | 100 | 1 | 1 000 | 0.0 | 79.9 % | 155.5 MB |
| mqtt | 1 000 | 0 | 10 000 | 0.0 | 143.15 % | 154.95 MB |
| mqtt | 1 000 | 1 | 10 000 | 0.0 | 281.16 % | 154.9 MB |
| mqtt | 1 000 | 2 | 10 000 | 0.0 | 429.09 % | 176.37 MB |
| mqtt | 10 000 | 1 | 100 000 | **81.37 %** | 574.47 % | 1 080.32 MB |
| kafka | 100 | all | 1 000 | 0.0 | 111.72 % | 654.43 MB |
| kafka | 1 000 | 0 | 10 000 | 0.0 | 119.2 % | 686.43 MB |
| kafka | 1 000 | 1 | 10 000 | 0.0 | 129.5 % | 650.5 MB |
| kafka | 1 000 | all | 10 000 | 0.0 | 120.89 % | 745.36 MB |
| kafka | 10 000 | all | 100 000 | **0.0 %** | 200.66 % | 1 039.07 MB |

> **Napomena o gubitku kod mqtt 10k**: 81.37% gubitka je **consumer-side backpressure**, ne broker saturacija. `ingest_emitted=3 000 000`, `ingest_dropped=0` (publisher je poslao sve), `storage_received≈561 000` (storage nije stigao da drain-uje QoS 1 PUBACK-ove). Razlog: QoS 1 zahteva publisher → broker → consumer PUBACK round-trip za svaku poruku; sa jednim storage procesom i 100k msg/s, broker-ov `max_inflight_messages=200 000` se popuni i publisher-ov queue overflow-uje. Sa 2+ storage konzumenta (MQTT bez consumer groups zahteva 2 zasebne subscription sesije) gubitak bi bio manji. Kafka to radi nativno preko consumer groups.
>
> **Napomena o CPU/RAM**: "ukupni" je suma docker stats za svih 5 projektnih kontejnera. Za fer komparaciju dva brokera treba čitati broker-izolovane vrednosti: Mosquitto ~4–6 MB RAM, Kafka ~700 MB RAM (JVM heap + page cache). Razlika je tradeoff za Kafka-in replay/durability.

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
| mqtt | **1** | 46.79 | 176.13 |
| kafka | **23** | 70.8 | 763.99 |

> Zaključak: **MQTT je 23× brži u recovery-u (1s vs 23s) u single-consumer setup-u.** Mehanizmi:
> - **MQTT** (QoS 1 + `clean_session=false`): broker drži poruke u queue-u dok je ingestion offline. Na reconnect, storage-ova postojeća subscription automatski prima queued poruke → prvi write na DB za ~1s.
> - **Kafka**: producer bufferuje lokalno, ALI storage (consumer group) mora da prođe kroz consumer group rebalance (~10–20s za jednog člana), pa tek onda drain-uje. Sa `acks=all` + RF=1 + single consumer, rebalance dominira recovery latency.
>
> **Tradeoff**: Kafka recovery je sporiji, ALI nakon recover-a nudi replay celog backlog-a do `log.retention.hours` unazad. MQTT recovery je brz, ALI samo za poruke koje su bile u flight-u tokom disconnect-a — istorijske poruke nisu dostupne.
>
> **Raniji rezultati u draftu ove sekcije (mqtt 23s, kafka 1s) bili su invertovani** — kafka run je izvršavan sa stale `.env` (broker=mqtt u ingestion log-u, kafka storage nije ništa primao, "1s" je bio artefakt starog topic stanja). Nakon pass-1 fix-a (`persist_env` + `ensure_kafka_topic`), run je ponovljen i dao prave vrednosti.

### Scenario C — Burst opterećenje

**Cilj**: 50 → 5 000 msg/s skok, backlog i recovery.

**Setup**: 60 s warm, 10 s burst, 60 s cool. `DB_ENABLED=true`, `KAFKA_ACKS=all`.

**Skripta**: `benchmarks/scenarios/scenario-c-burst.sh`.

**Dobijeni rezultati** (iz `results/tables/scenario-C.csv`):

| Broker | Lag (ms) | p95 lag (ms) | Peak backlog | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| mqtt | 6 | 22 | 171 | 42.29 | 185.23 |
| kafka | **3** | 23 | 403 | 56.24 | 799.03 |

> Zaključak: Kafka ima **duplo niži lag** (3ms vs 6ms) i **veći peak backlog** (403 vs 171) nego MQTT. Razlog: Kafka producer batch-uje (5ms linger), pa burst-od-5000 biva brzo upisan u topic i storage počinje da drain-uje ranije. MQTT QoS 1 čeka per-message PUBACK, pa burst stiže do storage sa ~3ms kašnjenja po poruci, koje se sabira u vidljivi lag. Peak backlog je veći za Kafka-u jer storage dobija više poruka u kraćem vremenskom prozoru, ALI brže ih i obrađuje — nije znak usporenja, već samo drugačiji buffering profil. RAM je 4.3× veći za Kafka (broker-only je ~700 MB vs ~5 MB; razlika je JVM heap + page cache).

### Scenario D — Real-Time alerting

**Cilj**: E2E latencija za kritične vrednosti.

**Setup**: `MODE=realtime`, `TIME_SCALE=100`, `INJECT_HIGH_TEMP=true`, `INJECT_AT_S=30`. `DB_ENABLED=false`.

**Skripta**: `benchmarks/scenarios/scenario-d-latency.sh`.

**Dobijeni rezultati** (iz `results/tables/scenario-D.csv`):

| Broker | Alerts | last_mean | E2E (s) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| mqtt | 17 | 90.53 | **8** | 66.45 | 164.49 |
| kafka | 18 | 90.5 | **3** | 65.06 | 780.07 |

> E2E alert latencija: Kafka 3s, MQTT 8s. Razlika od 5s dolazi od toga što Kafka producer batch-uje (5ms linger) i ne čeka per-message ack-ove, pa 100-msg injection burst stiže do storage → analytics u jednom API pozivu. MQTT QoS 1 čeka PUBACK pre svakog sledećeg publish-a, što dodaje ~50ms × 100 = 5s za 100-msg burst. TumblingWindow ima 10s fiksni prozor tako da je inherentna latencija ≤ 10s, ALI Kafka stiže do ALERT-a pre zatvaranja prozora, dok MQTT stiže tačno na granici. **Kod MQTT QoS 0 bi E2E bio niži** (jer nema PUBACK round-trip), ali bi se izgubila "at least once" garancija.
>
> Stari draft je imao negativne E2E vrednosti (-32, -33s) iz run-ova sa broken E2E formulom (pre commit-a 9e78227). Nakon fix-a i ponovnog pokretanja dobijene su 8s (mqtt) i 3s (kafka).

## 7. Uporedna tabela

Automatski se generiše u [`comparison-table.md`](comparison-table.md) od strane `scripts/make-report-tables.py` na osnovu `results/raw/*/`.

## 8. Analiza pouzdanosti

Detaljni odgovori na tri pitanja u [`analysis.md`](analysis.md).

1. **Zašto je MQTT idealan za edge uređaje?** i **zašto neadekvatan za istorijsku analitiku?**
2. **Zašto Kafka dominira u data-intensive cloud sistemima?** i **da li je realno pokretati je na hardverski ograničenim edge serverima?**
3. **Popuniti uporednu tabelu performansi** (vidi §7).

## 9. Zaključak

Na osnovu eksperimenata (sa čistim, rerunovanim podacima nakon pass-1 fix-ova):

- **Za edge i kontrolne telemetrijske kanale** (npr. MQTT poruke sa senzora): koristiti **MQTT**. Mali footprint, jednostavan QoS model, radi na lošim mrežama, i — što je ključno — **MQTT QoS 1 + clean_session=false daje ~1s recovery nakon disconnekta** (Scenario B), što je idealno za edge uređaje koji se bude iz sleep-a ili nakon gubitka mreže.
- **Za ingestion u data lake / data-intensive cloud sisteme**: koristiti **Kafka**. Durable log, replay, particionisanje, consumer groups. **Kafka drži 0% gubitka na 100k msg/s** (Scenario A) i ima **3s E2E alert latency** (Scenario D), jer decoupling-uje producer-ov throughput od consumer drain rate-a. MQTT QoS 1 single-consumer topiku gubi ~80% poruka na istom opterećenju zbog QoS 1 PUBACK backpressure-a.
- **Tradeoff na recovery**: Kafka recovery je sporiji (23s u single-consumer setup-u, zbog consumer group rebalance), ALI nudi replay celog backlog-a. MQTT recovery je brz, ALI samo za poruke u flight-u tokom disconnect-a.
- **Za hibridne edge-to-cloud sisteme**: postoje posrednici (EMQX, HiveMQ) koji izlažu MQTT ka uređajima a interno koriste Kafka topic; vredi razmotriti i **Redpanda** (Kafka-kompatibilan, C++ binarni, manji footprint).

Konkretna arhitekturna preporuka na osnovu merenja:

```
[Edge uređaji] --MQTT QoS 1 + clean=false--> [Mosquitto + bridge] --acks=all--> [Cloud Kafka] --> [Storage + Analytics]
```

Edge strana dobija brz recovery (1s); cloud strana dobija 0% gubitka na 100k+ msg/s i replay za analitiku.

## 10. Reference

- Specifikacija projekta: `/home/baladoodle/Baladoodle/Fakultet/IoTS/proj-2.md`.
- Dataset: F1 2020 telemetrija (korišćen u Proj-1).
- Biblioteke i verzije: vidi `services/*/IoT*.csproj` i `services/storage/package.json`.
- Generisanje izveštaja: `python3 scripts/make-report-tables.py`.

---

**Prilog**: [`appendix-run-logs.md`](appendix-run-logs.md) — pokretanje eksperimenata.
