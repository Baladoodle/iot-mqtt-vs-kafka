# Outline tehničkog izveštaja

`report/report.md` (srpski) prati sledeću strukturu. Poglavlja 1–4 su statična; 5–6 zavise od rezultata scenarija; 7 odgovara na tri pitanja iz specifikacije.

## 1. Uvod
- Kontekst kursa (Internet stvari i servisa, drugi projekat).
- Motivacija: publish/subscribe mikroservisi za IoT.
- Pregled problema: koji broker je bolji za koji workload.

## 2. Opis dataseta
- F1 telemetrija: 1 097 961 redova × 56 kolona, 20 pilota.
- Tipične vrednosti (npr. `engineTemperature ~ 90 °C`, `speed ~ 80 km/h`).
- Zašto ovaj dataset: realističan IoT scenario sa 20 heterogenih "uređaja".

## 3. Arhitektura sistema
- Tri mikroservisa, dva brokera, env-driven konfiguracija.
- Dijagram toka podataka (ASCII ili Markdown).
- Link ka [`architecture.md`](architecture.md).

## 4. Korišćene tehnologije
- Ingestion: ASP.NET Core / .NET 10.
- Storage: Node.js 22 + TypeScript.
- Analytics: ASP.NET Core / .NET 10.
- MQTT: Eclipse Mosquitto 2.0.
- Kafka: Apache Kafka 3.7.0 (KRaft).
- DB: PostgreSQL 16.
- Benchmark alati: emqtt-bench, kafka-producer-perf-test.sh, docker stats.

## 5. Podešavanje brokera
- Link ka [`broker-tuning.md`](broker-tuning.md).
- Obrazloženje svakog relevantnog parametra.

## 6. Eksperimenti
Jedno pod-poglavlje po scenariju. Struktura:

```
### Scenario A — Masivni IoT ingestion
**Setup**: <broker>, <N> uređaja, QoS/acks <level>, DB_ENABLED=false.
**Procedura**: ...
**Rezultati**: (tabela i opis)
**Zaključak**: ...
```

Rezultati se čitaju iz `results/tables/scenario-{A,B,C,D}.csv`.

## 7. Uporedna tabela
- Automatski generisana u `report/comparison-table.md` od strane `scripts/make-report-tables.py`.
- Stupci: Throughput, p95 latencija, CPU, RAM, Gubitak.
- Redovi: 4 scenarija × 2 brokera × QoS/acks nivoi.

## 8. Analiza pouzdanosti
Tri pitanja iz specifikacije:

1. **Zašto je MQTT idealan za edge uređaje?** i **zašto neadekvatan za istorijsku analitiku?**
2. **Zašto Kafka dominira u data-intensive cloud sistemima?** i **da li može na edge?**
3. **Popuniti uporednu tabelu.**

Vidi [`analysis.md`](../report/analysis.md) za detaljne odgovore.

## 9. Zaključak
- Kada koristiti koji broker.
- Budući rad: Confluent Schema Registry, Avro, Kubernetes orkestracija, IoT-specifični brokeri (EMQX, HiveMQ, Redpanda).

## 10. Reference
- Specifikacija projekta.
- Dataset poreklo.
- Verzije biblioteka i alata.
