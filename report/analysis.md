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

Vidi [`comparison-table.md`](comparison-table.md) — automatski se popunjava iz `results/raw/*/`.

**Stvarni rezultati ovog eksperimenta** (iz `results/tables/scenario-A.csv`):

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg | RAM avg |
|---|---|---|---|---|---|---|
| mqtt | 100 | 1 | 1 000 | 0.0 | 79.9 % | 155.5 MB |
| mqtt | 1 000 | 0 | 10 000 | 0.0 | 143.15 % | 154.95 MB |
| mqtt | 1 000 | 1 | 10 000 | 0.0 | 281.16 % | 154.9 MB |
| mqtt | 1 000 | 2 | 10 000 | 0.0 | 429.09 % | 176.37 MB |
| mqtt | 10 000 | 1 | 100 000 | **79.35 %** | 591.36 % | 1 077.72 MB |
| kafka | 1 000 | 0 | 10 000 | 0.0 | 119.2 % | 686.43 MB |
| kafka | 1 000 | 1 | 10 000 | 0.0 | 129.5 % | 650.5 MB |
| kafka | 1 000 | all | 10 000 | 0.0 | 120.89 % | 745.36 MB |
| kafka | 10 000 | all | 100 000 | **0.0 %** | 170.45 % | 1 093.38 MB |

### Ključni nalazi

1. **MQTT QoS 1 ne drži korak na 10k uređaja** — gubi 79% poruka. Razlog: svaki klijent ima zasebnu TCP konekciju + QoS1 zahteva 4 puta više paketa (PUBLISH → PUBACK); sa 10k × 10 msg/s = 100k msg/s, broker (jedan Mosquitto container) ne stiže da potvrdi sve PUBACK-ove u roku.

2. **Kafka acks=all drži 0% gubitka** čak i na 10k uređaja. Razlog: producer batch-uje poruke, broker čuva na disku, replication (RF=1 u single-broker setup-u, ali `acks=all` čeka da broker zapiše na log pre nego što potvrdi).

3. **MQTT QoS 2 ima 3× veći CPU nego QoS 0** (429% vs 143% pri 1k uređaja) jer QoS2 zahteva 4-step handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP) za svaku poruku.

4. **Kafka troši 4-7× više RAM-a nego MQTT** (~700 MB vs ~155 MB) zbog JVM heap + page cache. Ovo je tradeoff za durability i replay.

5. **Recovery od disconnekta**: Kafka 1s, MQTT 23s (Scenario B). Kafka consumer offset reposition je trenutan; MQTT QoS1+clean_session=false drži poruke u queue-u ali klijent mora sekvencijalno da ih drain-uje.

6. **E2E alert latency** (Scenario D): Kafka 6s, MQTT 10s. TumblingWindow ima fiksni 10s prozor tako da je inherentna latencija ≤ 10s, ali Kafka stiže do ALERT-a 4s brže jer nema dodatnog hop-a kroz Mosquitto.

### Praktična preporuka

- **Edge → Cloud ingestion**: koristiti **MQTT** zbog lightweight protokola, QoS, LWT, session persistence.
- **Cloud → Storage / Analytics**: koristiti **Kafka** zbog durability, replay, consumer groups.
- **Edge gateway**: pokretati **Mosquitto + bridge plugin** koji automatski prosleđuje MQTT poruke u Kafka topic. Time se dobija "best of both worlds": jednostavan edge protokol + cloud-scale persistencija.
