# Eksperimentalni scenariji

Specifikacija (proj-2.md, §4) zahteva četiri scenarija. Ovaj dokument opisuje svaki sa setup-om, procedurom i očekivanim rezultatima.

## Scenario A — Masivni IoT ingestion

**Cilj**: Maksimum throughput (msg/s) i procenat izgubljenih poruka za 100, 1 000 i 10 000 uređaja.

**Podešavanje**:
- `DB_ENABLED=false` (Storage servis koristi blackhole; broker je usko grlo).
- Za MQTT: testirati QoS 0, 1, 2.
- Za Kafka: testirati acks 0, 1, all.
- `RATE = NUM_DEVICES × 10` (npr. 10 000 uređaja → 100 000 msg/s teorijski).
- `DURATION_S = 30`.

**Skripta**: `benchmarks/scenarios/scenario-a-throughput.sh mqtt 100 1` itd.

**Očekivani rezultati**:
- MQTT QoS 0: najbrži, ali bez garancije.
- MQTT QoS 2: najsporiji (4-way handshake po poruci).
- Kafka acks=0: sličan QoS 0, ali gubitke krijemo u log-u.
- Kafka acks=all: sporiji, ali durable.

**Metrički output**:
- `results/raw/scenario-A-{broker}-{N}-{level}/`
  - `ingestion.log` (emitted/dropped)
  - `storage.log` (received/persisted)
  - `stats.csv` (CPU/RAM po kontejneru)
  - `*_metrics.txt` (Prometheus dump)

## Scenario B — Edge connectivity failures

**Cilj**: Oporavak nakon 30 s `docker network disconnect` na ingestion kontejneru.

**Podešavanje**:
- `MQTT_CLEAN_SESSION=false` za fer poređenje (inače bi MQTT izgubio sve QoS 1/2 poruke tokom outage-a).
- `Kafka` automatski zadržava poruke u topic-u (recovery = ponovno uspostavljanje konekcije ingestion → broker, konzumenti ne pate).
- `RATE=500`, `DURATION_S=120`.
- Disconnect na 30. sekundi, reconnect na 60. sekundi.

**Skripta**: `benchmarks/scenarios/scenario-b-disconnect.sh mqtt` / `kafka`.

**Očekivani rezultati**:
- **MQTT clean_session=false**: poruke se čuvaju na brokeru dok je ingestion offline; storage ih konzumira posle reconnect-a. Persistira QoS 1 i 2 poruke u queue-u. QoS 0 se gubi.
- **MQTT clean_session=true**: poruke se trenutno odbacuju (lekcija: za production edge, koristiti clean=false + persistent storage).
- **Kafka**: ingestion proizvodi u local buffer; posle reconnect-a, sve poruke stižu do brokera. Storage i Analytics ne pate.

**Recovery metrika**: vreme od `docker network connect` do prvog upisa u Postgres / prvog ALERT-a.

## Scenario C — Burst opterećenje

**Cilj**: 50 → 5 000 msg/s skok, meri formiranje reda čekanja, backpressure i vreme oporavka.

**Podešavanje**:
- 60 s warm-up (RATE=50), 10 s burst (RATE=5000), 60 s cool-down (RATE=50).
- `DB_ENABLED=true` da backpressure propagira kroz Storage u broker.
- `KAFKA_ACKS=all` (najgori slučaj za proizvođača).

**Skripta**: `benchmarks/scenarios/scenario-c-burst.sh mqtt` / `kafka`.

**Očekivani rezultati**:
- **MQTT QoS 1/2**: broker queue može da apsorbuje kratki burst; posle burst-a, drainage traje dovoljno dugo da storage dođe do catch-up-a.
- **Kafka acks=all**: producer blokira na `acks`; queue raste; drainage je sporiji jer svaka poruka čeka 4 ISR-a (mi imamo 1, pa je slično).
- Backlog merimo kroz `storage_lag_ms` (zadnji batch) i `storage_p95_lag_ms`.

## Scenario D — Real-Time alerting

**Cilj**: End-to-end latencija od publish-a kritične vrednosti do ALERT log-a u Analytics-u.

**Podešavanje**:
- `MODE=realtime`, `TIME_SCALE=100` (ubrzanje 100×).
- `INJECT_HIGH_TEMP=true`, `INJECT_AT_S=30`.
- `DB_ENABLED=false` (čist putanja: publisher → broker → analytics).
- `NUM_DEVICES=10` (mali promet).

**Skripta**: `benchmarks/scenarios/scenario-d-latency.sh mqtt` / `kafka`.

**Očekivani rezultati**:
- Svaki servis loguje `t_emit` (publish), `t_persist` (DB upis), `t_alert` (analitika). Za Scenario D bez DB, koristimo `t_emit` ↔ `t_alert`.
- Očekivana latencija:
  - MQTT QoS 1 + 1 konzument: < 100 ms tipično.
  - Kafka acks=1: < 50 ms.
  - Kafka acks=all: < 200 ms (čekanje na persist).
- Latencija raste sa `RATE` jer se prozori ne zatvaraju sve dok ne stigne event sa novijim `t_emit`.

## Generisanje izveštaja

Posle pokretanja svih scenarija:

```bash
python3 scripts/make-report-tables.py
cat report/comparison-table.md
```

Skripta parsira `results/raw/*` logove i generiše `results/tables/scenario-{A,B,C,D}.csv` i `report/comparison-table.md`.
