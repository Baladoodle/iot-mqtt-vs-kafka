# Uporedna tabela performansi — auto-generisano

Stupci: Throughput (msg/s), gubitak (%), prosečni CPU (%), prosečni RAM (MB), alerti.


## Scenario A — Throughput po uređajima i QoS/ACKS

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|
| kafka | 10000 | acksall | 100000.0 | 0.0 | 200.66 | 1039.07 |
| mqtt | 10000 | qos1 | 100000.0 | 81.37 | 574.47 | 1080.32 |

## Scenario B — Disconnect / Recovery

| Broker | Uređaji | Trajanje (s) | Recovery (s) | E2E (s) | ALERT nakon reconnect | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|---|
| kafka | n/a | None | 23 | None | None | 70.8 | 763.99 |
| mqtt | n/a | None | 1 | None | None | 46.79 | 176.13 |

## Scenario C — Burst / Backlog

| Broker | Uređaji | Trajanje (s) | Lag (ms) | p95 lag (ms) | Peak backlog | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|---|
| kafka | n/a | None | 3 | 23 | 403 | 56.24 | 799.03 |
| mqtt | n/a | None | 6 | 22 | 171 | 42.29 | 185.23 |

## Scenario D — E2E latencija (alert)

| Broker | Alerts | last_mean | E2E (s) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| kafka | 18 | 90.5 | 3 | 65.06 | 780.07 |
| mqtt | 17 | 90.53 | 8 | 66.45 | 164.49 |
