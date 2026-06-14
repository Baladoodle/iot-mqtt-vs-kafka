# Analiza pouzdanosti

Tri inženjerska pitanja iz specifikacije, sa detaljnim odgovorima.

## 1. Zašto je MQTT idealan za edge uređaje, a neadekvatan za istorijsku analitiku?

### Prednosti MQTT-a za edge

**a) Mali binarni i footprint**
- Eclipse Mosquitto: < 500 KB binarni, radi na uređajima sa 32 MB RAM-a.
- Klijentske biblioteke (mqtt.js, MQTTnet, Paho) su lagane (< 200 KB).
- Kontrast: Kafka klijent (librdkafka) je 5–10 MB; broker sa JRE zahteva ≥ 1 GB.

**b) Topic-tree fan-out**
- Hijerarhijski namespace (`iot/telemetry/{device_id}/temp`) omogućava granularnu pretplatu bez filtera na strani klijenta.
- Broker interno optimizuje pretplate u efikasnu strukturu.

**c) QoS za nepouzdane mreže**
- QoS 0: "fire and forget" — gubitak prihvatljiv za senzorske metrike.
- QoS 1: "at least once" — broker čuva poruke dok god klijent ne potvrdi; ključno za edge uređaje koji se bude iz sleep-a.
- QoS 2: "exactly once" — retko neophodan, ali podržan.

**d) Last Will and Testament (LWT)**
- Klijent može registrovati "oporuka" koja se automatski objavljuje kad se klijent neočekivano diskonektuje. Korisno za otkrivanje "mrtvih" senzora.

**e) Session persistence (clean_session=false)**
- Poruke QoS 1/2 + persistentne sesije čuvaju se na brokeru dok se klijent ponovo ne poveže. Edge uređaj može biti offline satima bez gubitka.

**f) TCP + WebSocket**
- Jednostavan transport, radi i iza NAT-a (HTTP/HTTPS tunneling nije potreban), radi u pretraživaču.

### Nedostaci MQTT-a za istorijsku analitiku

**a) Bez replej-a**
- Broker ne čuva istoriju topica (osim QoS 1/2 + clean=false za offline klijente). Ako klijent propusti poruku jer je došao posle objave, nema načina da je dobije.
- Za analitiku "šta se desilo u 14:32:18" — nepostojeće.

**b) Bez offset modela**
- MQTT nema "offset" ili "position" koncept. Ako analitički servis padne, mora da se re-abonuje i nada da će broker imati još poruka (obično nema).

**c) Bez particionisanja**
- Ne postoji paralelizam na strani konzumenta: svi pretplaćeni klijenti dobijaju isti tok. Ako želimo 4 analitičara paralelno, ne možemo ih particionisati kao u Kafka-i.

**d) Retention je vezan za memoriju brokera**
- QoS 1/2 poruke se drže u RAM-u (ili fajlu, ako je `persistence true`). Ne postoji konfigurabilna retention politika kao `log.retention.hours` u Kafka-i.

**e) Bez kompresije/serijalizacije na brokeru**
- Broker samo prosleđuje payload bajtove; ne zna šta je unutra. Za analitiku koja želi "sve evente tipa X" moraš da filtriraš na konzumentu.

**f) "Consumer groups" ne postoje**
- Svaki pretplaćeni klijent dobija **sve** poruke. Ako imaš 10 analitičara, svi dobijaju iste podatke. To je "broadcast" model, ne "work queue".

### Zaključak

MQTT je optimalan za **kontrolne telemetrijske kanale** sa edge uređaja prema serveru. Kada poruke stignu do centralnog sistema i treba da se čuvaju za analizu, treba ih proslediti u **Kafka** (ili drugu perzistentnu, replikovanu log strukturu).

## 2. Zašto Kafka dominira u data-intensive cloud sistemima, i da li je realno na edge-u?

### Prednosti Kafka-e u data-intensive cloud-u

**a) Durable, particionisan log**
- Svaka poruka ide na disk (ili SSD), sa `acks=all` čak i replikacija.
- Poruke se čuvaju retention period (default 7 dana, konfigurabilno).
- Particije omogućavaju horizontalni paralelizam: 10 particija = 10 paralelnih konzumenata.

**b) Offset model omogućava replay**
- Svaki consumer group čuva svoj offset.
- Analitički servis može da resetuje offset na "from-beginning" i reprocesira celu istoriju.
- Nova verzija modela može da se pokrene paralelno sa starom (blue/green deployment).

**c) Consumer groups = work queue + broadcast**
- U jednoj grupi, svaka poruka ide tačno jednom consumeru (work queue za load balancing).
- Više grupa = svaka grupa dobija sve (broadcast za paralelnu obradu).
- Ovo je suštinski "oba modela u jednom".

**d) Ekosistem**
- Kafka Streams, ksqlDB, Schema Registry, Kafka Connect (za povezivanje sa bazama).
- SaaS ponude: Confluent Cloud, AWS MSK, Azure Event Hubs.
- Ogroman broj klijenata u svim jezicima.

**e) Backpressure i streaming**
- Producer blokira kada broker kaže "ne mogu brže" — prirodan backpressure.
- Consumer kontroliše koliko brzo vuče.

### "Cena" Kafka skalabilnosti

| Resurs | Tipična potrošnja |
|---|---|
| RAM | 1–4 GB za broker (heap + page cache) |
| Disk | retention × ingest rate; ~1 TB/dan nije neuobičajeno |
| CPU | 4–8 jezgara po brokeru za 50 MB/s ingest |
| JVM overhead | GC pauze, JIT warm-up |
| Operativna složenost | particionisanje, rebalansiranje, schema evolucija, monitoring |

### Da li je realno na edge-u?

**Tradicionalna Kafka**: NE. Minimalni "production" Kafka klaster (3 brokera) traži 3 × (4 GB RAM, 2 CPU, 100 GB SSD). Edge uređaj (npr. industrijski gateway) tipično ima 512 MB RAM i 1 CPU.

**Lakše alternative**:

| Broker | Binarni | RAM | Kafka kompatibilan |
|---|---|---|---|
| **Redpanda** | C++ single binary | 200 MB | Da (Kafka API) |
| **Apache Pulsar** | Java | 1 GB | Ne (drugačiji API) |
| **NATS JetStream** | Go | 50 MB | Ne (subject-based) |
| **Mosquitto + plugin za persistenciju** | C | 10 MB | N/A |

**Praktično rešenje** za edge-to-cloud:

```
[Senzor] --MQTT--> [Edge gateway: Mosquitto + bridge] --Kafka--> [Cloud Kafka cluster]
```

Bridge plugin (mosquitto-clients, EMQX Bridge, HiveMQ Bridge) preuzima MQTT poruke i publish-uje ih kao Kafka poruke. Time se dobija najbolje iz oba sveta: jednostavan edge protokol + cloud-scale persistencija.

### Zaključak

Kafka je "teška" ali opravdana investicija **na cloud strani** gde je retention i replay kritičan. **Na edge-u** koristiti MQTT, a most ka Kafka-i praviti na granici.

## 3. Uporedna tabela performansi

Vidi [`comparison-table.md`](comparison-table.md) — automatski se popunjava iz `results/raw/*/`. Dole je dopunjena tabela sa svim pokrenutim kombinacijama (uključujući `kafka 100/acksall` koji je u prvoj iteraciji imao 100% gubitka zbog topic-creation race condition-a; nakon fix-a u pass-1 prolazi sa 0% gubitka).

**Stvarni rezultati ovog eksperimenta** (iz `results/tables/scenario-A.csv`, poslednji validan run po (broker, uređaji, level)):

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU ukupni | RAM ukupni | Broker CPU | Broker RAM |
|---|---|---|---|---|---|---|---|---|
| mqtt | 100 | 1 | 1 000 | 0.0 | 79.9 % | 155.5 MB | ~30 % | ~4 MB |
| mqtt | 1 000 | 0 | 10 000 | 0.0 | 143.15 % | 154.95 MB | ~25 % | ~5 MB |
| mqtt | 1 000 | 1 | 10 000 | 0.0 | 281.16 % | 154.9 MB | ~50 % | ~5 MB |
| mqtt | 1 000 | 2 | 10 000 | 0.0 | 429.09 % | 176.37 MB | ~80 % | ~6 MB |
| mqtt | 10 000 | 1 | 100 000 | **81.37 %** | 574.47 % | 1 080.32 MB | ~53 % | ~0.2 MB* |
| kafka | 100 | all | 1 000 | 0.0 | 111.72 % | 654.43 MB | ~25 % | ~350 MB |
| kafka | 1 000 | 0 | 10 000 | 0.0 | 119.2 % | 686.43 MB | ~30 % | ~360 MB |
| kafka | 1 000 | 1 | 10 000 | 0.0 | 129.5 % | 650.5 MB | ~35 % | ~360 MB |
| kafka | 1 000 | all | 10 000 | 0.0 | 120.89 % | 745.36 MB | ~35 % | ~400 MB |
| kafka | 10 000 | all | 100 000 | **0.0 %** | 200.66 % | 1 039.07 MB | ~85 % | ~700 MB |

\* Mosquitto na 10k uređaja drži payload-ove u memoriji samo za QoS 1/2 klijente sa `clean_session=false`; za scenario A (clean=true default) gotovo sve prolazi kroz RAM bez zadržavanja.

> **Napomena o CPU/RAM kolonama**: "CPU ukupni" i "RAM ukupni" sabiraju docker stats za svih 5 projektnih kontejnera (ingestion, broker, storage, analytics, postgres). "Broker CPU" i "Broker RAM" su izolovani za sam broker proces — ovo je jedina poštena komparacija dva brokera. Storage i ingestion troše značajan deo CPU na 100k msg/s (mqtt: ingestion 234–276%, storage 60–65%; kafka: ingestion manji zbog batchovanja).

### Ključni nalazi

1. **MQTT QoS 1 gubi ~81% poruka na 10k uređaja — ALI to je consumer-side backpressure, ne broker saturacija.** Publisher (`ingest_emitted=3 000 000`, `ingest_dropped=0`) JE poslao sve poruke i broker IH JE prihvatio. Storage je primio samo 619 915 (mqtt 10k stari broj) / 561 000 (mqtt 10k novi broj). Razlog: QoS 1 zahteva da broker čeka PUBACK od **svakog** konzumenta pre nego što pošalje sledeći PUBLISH; sa jednim storage procesom koji batch-uje 500/200ms, PUBACK round-trip postaje usko grlo, brokerov `max_inflight_messages=200 000` se popuni, publisher-ov queue overflow-uje. Da je bilo 2+ storage konzumenta (MQTT ne podržava consumer groups, ali se može postići sa 2 odvojene subscription sesije), gubitak bi bio znatno manji. Dakle: **MQTT QoS 1 na 100k msg/s zahteva horizontalno skaliranje konzumenata**, dok Kafka to radi nativno preko consumer groups.

2. **Kafka `acks=all` drži 0% gubitka na 10k uređaja** jer producer batch-uje poruke i ne čeka pojedinačne konzumenta-ack-ove — broker čuva poruke na disku i consumer group-ovi ih nezavisno konzumiraju. Ovo je suštinska razlika u arhitekturi: Kafka decoupling-uje producer-ov throughput od consumer-ovog drain rate-a.

3. **MQTT QoS 2 ima 3× veći broker-CPU nego QoS 0** (80% vs 25% pri 1k uređaja) jer QoS2 zahteva 4-step handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP) za svaku poruku. Ovo je protokol-overhead, ne implementaciona specifičnost.

4. **Kafka broker troši ~100× više RAM-a nego Mosquitto** (~700 MB vs ~4–6 MB), a Kafka broker-CPU je ~1.6× veći (85% vs 53% na 10k uređaja). RAM razlika je zbog JVM heap + page cache — Kafka drži segmente loga u RAM za brz pristup. Ovo je **tradeoff za durability i replay**: Kafka može da vrati istoriju do `log.retention.hours` unazad, MQTT ne može (osim za QoS 1/2 + clean=false offline klijente).

5. **Recovery od disconnekta (Scenario B) — MQTT je 23× brži od Kafka-e** u single-consumer setup-u (1s vs 23s). Mehanizmi:
   - **MQTT** (QoS 1 + `clean_session=false`): broker drži poruke u queue-u dok je ingestion offline. Na reconnect, storage-ova postojeća subscription automatski prima queued poruke → prvi write na DB za ~1s.
   - **Kafka**: producer bufferuje lokalno, ALI storage (consumer) mora da prođe kroz consumer group rebalance (~10–20s za jednog člana), pa tek onda drain-uje. Sa `acks=all` + RF=1 + single consumer, rebalance dominira recovery latency.
   
   **Tradeoff**: Kafka recovery je sporiji, ALI nakon recover-a nudi replay celog backlog-a do retention period-a. MQTT recovery je brz, ALI samo za poruke koje su bile u flight-u tokom disconnect-a — istorijske poruke nisu dostupne.

6. **E2E alert latency (Scenario D)**: Kafka 3s, MQTT 8s. TumblingWindow ima fiksni 10s prozor tako da je inherentna latencija ≤ 10s, ALI Kafka stiže do ALERT-a 5s brže. Razlog: Kafka producer batch-uje (5ms linger) i ne čeka per-message ack-ove, pa injection event brže stiže do storage → analytics. MQTT QoS 1 čeka PUBACK pre svakog sledećeg publish-a, što dodaje ~3–5s kašnjenja za 100-msg burst na 10k-device single-subscriber topiku.

### Praktična preporuka

- **Edge → Cloud ingestion**: koristiti **MQTT** zbog lightweight protokola, QoS, LWT, session persistence. **Recovery scenario (B) je MQTT-ov adut**: QoS 1 + clean_session=false daje ~1s recovery, što je idealno za edge uređaje koji se bude iz sleep-a.
- **Cloud → Storage / Analytics**: koristiti **Kafka** zbog durability, replay, consumer groups. **Throughput (Scenario A) i E2E (Scenario D) su Kafka-ini aduti**: 0% gubitka na 100k msg/s i 3s alert latency, jer Kafka decoupling-uje producer-ov throughput od consumer drain rate-a.
- **Ako se MQTT koristi na 100k+ msg/s u cloud-u**: planirati **N storage konzumenata** (svaki sa zasebnom subscription) da bi se izbegao single-consumer PUBACK backpressure. Alternativno, koristiti QoS 0 za high-throughput firehose i QoS 1 samo za kritične kontrolne kanale.
- **Edge gateway**: pokretati **Mosquitto + bridge plugin** koji automatski prosleđuje MQTT poruke u Kafka topic. Time se dobija "best of both worlds": jednostavan edge protokol + cloud-scale persistencija + brz MQTT recovery za edge + Kafka replay za cloud analytics.
