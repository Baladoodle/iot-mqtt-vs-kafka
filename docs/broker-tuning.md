# Podešavanje brokera

## Mosquitto ([`brokers/mosquitto/config/mosquitto.conf`](../brokers/mosquitto/config/mosquitto.conf))

Ključna podešavanja i zašto:

| Parametar | Vrednost | Obrazloženje |
|---|---|---|
| `listener 1883` | TCP port | Standardni MQTT port. |
| `allow_anonymous true` | Pojednostavljenje | Za scenario testiranje; za produkciju dodati `password_file`. |
| `persistence true` | Perzistencija uključena | Neophodno za Scenario B sa `clean_session=false`. |
| `max_queued_messages 1 000 000` | Visok limit | Da broker ne odbacuje QoS 1/2 poruke dok je klijent offline. Default je 100. |
| `max_inflight_messages 200 000` | Visok limit | Da ingestion ne bude blokiran u QoS 1/2 publish tokovima. Default je 20. |
| `max_queued_bytes 1 073 741 824` | 1 GB | Zaštita od unbounded rasta memorije. |
| `max_connections 100 000` | Visok | 10 000 uređaja × QoS × replikacije. |
| `max_client_id_length 64` | Standard | Dovoljno za `device_id = "{pilot}-{replica}"`. |

### Po čemu se razlikuje od MQTT QoS-a

- **QoS 0** (at most once): publish ide bez potvrde. Najbrži, ali gubi se poruka ako se klijent diskonektuje u letu.
- **QoS 1** (at least once): broker čuva poruku dok klijent ne potvrdi `PUBACK`. Može doći do duplikata.
- **QoS 2** (exactly once): 4-way handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP). Najskuplji, najsporiji.

## Kafka ([`brokers/kafka-kraft/config/server.properties`](../brokers/kafka-kraft/config/server.properties))

KRaft single-node setup (bez Zookeeper-a).

| Parametar | Vrednost | Obrazloženje |
|---|---|---|
| `process.roles=broker,controller` | Kombinovano | Jedan proces je i broker i controller. Za multi-node bi se razdvojili. |
| `controller.quorum.voters=1@localhost:9093` | Single-node | Za produkciju 3+ čvora. |
| `listeners=PLAINTEXT://:9092,CONTROLLER://:9093` | Dva porta | 9092 za klijente, 9093 za kontrolni kanal. |
| `auto.create.topics.enable=false` | Isključeno | Kontrola: kreiramo topic ručno sa `init-topics.sh`. |
| `num.partitions=4` | Default | Za Scenario A testiranje 10 000 uređaja kroz 4 particije → consumer lag se može paralelizovati. |
| `default.replication.factor=1` | Single-node | Za produkciju 3+. |
| `log.retention.minutes=60` | 1 sat | Za Scenario B je dovoljno. Za produkciju znatno duže. |
| `log.segment.bytes=1GB` | Veliki segment | Manji broj fajlova = brži recovery. |

### Po čemu se razlikuje Kafka `acks`

- **`acks=0`**: producer ne čeka nikakvu potvrdu. Najbrži, ali broker može izgubiti poruku pre nego što je zapiše.
- **`acks=1`**: leader potvrđuje čim primi poruku (pre replikacije). Brz, ali gubitak ako se leader sruši pre replikacije.
- **`acks=all`** (ili `-1`): svi in-sync replike potvrđuju. Najsporiji, najsigurniji.

### Idempotent producer

Za `acks=all`, postavili smo `enable.idempotence=true` u ingestion servisu. Ovo garantuje da duplikat (npr. retry zbog timeout-a) neće proizvesti duplu poruku u log-u.

## Razlike u "filozofiji" između MQTT i Kafka

| Dimenzija | MQTT | Kafka |
|---|---|---|
| Perzistencija | Opciona (QoS 1/2 + persistence) | Podrazumevana (svaka poruka ide na disk) |
| Retention | Po pravilu nema (broker zaboravlja čim klijent potvrdi) | Konfigurabilna (default 7 dana) |
| Replay | Nemoguć (broker ne čuva istoriju za QoS 0; za QoS 1/2 samo za offline klijente) | Uvek moguć (svaki consumer može resettovati offset) |
| Consumer model | Fan-out: svaki pretplaćeni klijent dobija sve poruke | Consumer groups: svaka poruka ide jednom consumeru u grupi |
| Topic model | Hijerarhijski (npr. `iot/telemetry/+/temp`) | Flat (samo ime topica) |
| Sharding/Particije | Nema | Ključ poruke određuje particiju |
| Operativna složenost | Niska (1 proces) | Srednja (1+ broker + controller state) |
| Edge pogodnost | Visoka (manji binary, manje RAM) | Niska (~500MB JVM/heap minimum) |
