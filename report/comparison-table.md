# Uporedna tabela performansi — auto-generisano

Stupci: Throughput (msg/s), gubitak (%), prosečni CPU (%), prosečni RAM (MB), alerti.


## Scenario A — Throughput po uređajima i QoS/ACKS

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|
| kafka | 100 | acks0 | 1000.0 | n/a | 58.52 | 523.24 |
| kafka | 100 | acks1 | 1000.0 | n/a | 53.6 | 527.16 |
| kafka | 1000 | acksall | 10000.0 | n/a | 63.15 | 603.95 |
| kafka | 10000 | acksall | 100000.0 | n/a | 84.51 | 880.08 |
| mqtt | 100 | qos0 | 1000.0 | n/a | 50.48 | 144.47 |
| mqtt | 100 | qos1 | 1000.0 | n/a | 81.35 | 187.23 |
| mqtt | 100 | qos2 | 1000.0 | n/a | 102.38 | 157.29 |
| mqtt | 1000 | qos1 | 10000.0 | n/a | 283.48 | 155.9 |
| mqtt | 10000 | qos1 | 100000.0 | n/a | 574.91 | 1013.63 |

## Scenario D — E2E latencija

| Broker | Alerts | last_mean | CPU avg | RAM avg |
|---|---|---|---|---|
