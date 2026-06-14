# Uporedna tabela performansi — auto-generisano

Stupci: Throughput (msg/s), gubitak (%), prosečni CPU (%), prosečni RAM (MB), alerti.


## Scenario A — Throughput po uređajima i QoS/ACKS

| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|
| kafka | 10000 | acksall | 100000.0 | 0.0 | 170.45 | 1093.38 |
| mqtt | 10000 | qos1 | 100000.0 | 79.35 | 591.36 | 1077.72 |

## Scenario B — Disconnect / Recovery

| Broker | Uređaji | Trajanje (s) | Recovery (s) | E2E (s) | ALERT nakon reconnect | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|---|
| kafka | n/a | None | 1 | None | None | 66.42 | 529.81 |
| mqtt | n/a | None | 23 | None | None | 40.31 | 206.49 |

## Scenario C — Burst / Backlog

| Broker | Uređaji | Trajanje (s) | Lag (ms) | p95 lag (ms) | Peak backlog | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|---|---|
| kafka | n/a | None | 6 | 22 | 343 | 52.94 | 715.07 |
| mqtt | n/a | None | 6 | 25 | 339 | 35.77 | 191.6 |

## Scenario D — E2E latencija (alert)

| Broker | Alerts | last_mean | E2E (s) | CPU avg (%) | RAM avg (MB) |
|---|---|---|---|---|---|
| kafka | 17 | 90.71 | 6 | 63.17 | 672.96 |
| mqtt | 17 | 90.46 | 10 | 63.93 | 186.98 |
