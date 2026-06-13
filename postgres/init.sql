-- =================================================================
-- Proj-2 — Postgres šema za Data Storage Service
-- =================================================================

CREATE TABLE IF NOT EXISTS telemetry (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    pilot_index INT,
    replica INT,
    "sessionTime" REAL,
    "frameIdentifier" INT,
    "speed" REAL,
    "engineTemperature" REAL,
    "tyresSurfaceTemperature" REAL,
    "worldPositionX" REAL,
    "worldPositionY" REAL,
    "worldPositionZ" REAL,
    t_emit TIMESTAMPTZ NOT NULL,
    t_persist TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB
);

CREATE INDEX IF NOT EXISTS telemetry_device_emit_idx
    ON telemetry (device_id, t_emit);

CREATE INDEX IF NOT EXISTS telemetry_emit_idx
    ON telemetry (t_emit);
