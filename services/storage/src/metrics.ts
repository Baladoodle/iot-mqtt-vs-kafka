import { Counter, Gauge, Registry, collectDefaultMetrics } from './metrics-prom';

const reg = new Registry();
collectDefaultMetrics({ register: reg });

/**
 * Minimalan Prometheus-like metrics output (za parser bez eksternih deps-a).
 * Format: comment + HELP + TYPE + redovi.
 */
export const metrics = {
    received: new Counter('storage_received_total', 'Poruke primljene sa brokera', reg),
    persisted: new Counter('storage_persisted_total', 'Poruke upisane u Postgres', reg),
    dropped: new Counter('storage_dropped_total', 'Poruke odbačene (DB error, JSON parse fail)', reg),
    batches: new Counter('storage_batches_total', 'Broj batch flush-eva', reg),
    lastBatchSize: new Gauge('storage_last_batch_size', 'Veličina poslednjeg batcha', reg),
    lastLagMs: new Gauge('storage_lag_ms', 'Lag = t_persist - t_emit (ms), zadnji batch', reg),
    p95LagMs: new Gauge('storage_p95_lag_ms', 'p95 lag zadnjih 1000 batcheva', reg),
    lossPct: new Gauge('storage_loss_pct', 'Procenat gubitka (received - persisted) / received', reg),
    register: reg,
};
