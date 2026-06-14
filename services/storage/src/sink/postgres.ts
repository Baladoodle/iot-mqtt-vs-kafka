import { Client } from 'pg';
import pino from 'pino';
import { TelemetryEvent, TelemetryRow } from '../models';
import { metrics } from '../metrics';

const log = pino({ name: 'pg-sink' });

/**
 * Postgres sink. Prikupi batch, uradi jedan INSERT sa multi-VALUES
 * (brže od individualnih upita; COPY bi bilo brže ali zahteva COPY IN).
 */
export class PostgresSink {
    private client: Client;
    private connected = false;

    constructor(private readonly url: string) {
        this.client = new Client({ connectionString: url });
    }

    async start(): Promise<void> {
        await this.client.connect();
        this.connected = true;
        // Proveri da li tabela postoji
        const r = await this.client.query(
            `SELECT 1 FROM information_schema.tables WHERE table_name = 'telemetry'`,
        );
        if (r.rowCount === 0) {
            log.warn('Tabela telemetry ne postoji — kreiranje');
            await this.createSchema();
        }
        log.info({ url: this.url.replace(/:[^:@]+@/, ':***@') }, 'Postgres connected');
    }

    private async createSchema(): Promise<void> {
        await this.client.query(`
            CREATE TABLE IF NOT EXISTS telemetry (
              id BIGSERIAL PRIMARY KEY,
              device_id TEXT NOT NULL,
              pilot_index INT,
              sessionTime REAL,
              frameIdentifier INT,
              speed REAL,
              engineTemperature REAL,
              tyresSurfaceTemperature REAL,
              worldPositionX REAL,
              worldPositionY REAL,
              worldPositionZ REAL,
              t_emit TIMESTAMPTZ NOT NULL,
              t_persist TIMESTAMPTZ NOT NULL DEFAULT now(),
              payload JSONB
            );
            CREATE INDEX IF NOT EXISTS telemetry_device_emit_idx ON telemetry (device_id, t_emit);
            CREATE INDEX IF NOT EXISTS telemetry_emit_idx ON telemetry (t_emit);
        `);
    }

    /**
     * Upisuje batch u jednom upitu. Vraća broj uspešno upisanih redova.
     *
     * Implementacioni detalj: preferira se multi-VALUES (jedan round-trip za
     * ceo batch), ali ako Npgsql prijavi protocol-level grešku tipa
     * "bind message has N parameter formats but 0 parameters" (primećeno u
     * Kafka scenariju B posle ~18k batcheva), fallback-ujemo na per-row
     * INSERT u transakciji. Sporije, ali ne gubi batch.
     */
    async writeBatch(events: TelemetryEvent[]): Promise<number> {
        if (events.length === 0) return 0;
        const tPersist = new Date();

        const cols = 'device_id, pilot_index, replica, "sessionTime", "frameIdentifier", "speed", "engineTemperature", "tyresSurfaceTemperature", "worldPositionX", "worldPositionY", "worldPositionZ", t_emit, t_persist, payload';
        const placeholders: string[] = [];
        const values: any[] = [];
        let p = 1;
        for (const e of events) {
            placeholders.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}, to_timestamp($${p++}), to_timestamp($${p++}), $${p++})`);
            values.push(
                e.device_id,
                e.pilot_index,
                e.replica ?? 0,
                e.sessionTime,
                e.frameIdentifier,
                e.speed,
                e.engineTemperature,
                e.tyresSurfaceTemperature,
                e.worldPositionX,
                e.worldPositionY,
                e.worldPositionZ,
                e.t_emit / 1000,
                tPersist.getTime() / 1000,
                JSON.stringify(e),
            );
        }

        const sql = `INSERT INTO telemetry (${cols}) VALUES ${placeholders.join(',')}`;
        try {
            // Defensive sanity check: placeholders i values MORAJU biti u
            // saglasnosti. Ako nisu (npr. kolekcija parcijalno popunjena),
            // ne šaljemo pokvaren upit — fallback-ujemo na per-row.
            // placeholders.length = events.length (jedan "(...)" string po redu)
            // values.length = events.length * 14 (14 $N referenci po redu)
            const expectedValues = events.length * 14;
            if (placeholders.length !== events.length || values.length !== expectedValues) {
                throw new Error(
                    `values/placeholders mismatch: placeholders=${placeholders.length}, values=${values.length}, expected ${expectedValues}`,
                );
            }
            const r = await this.client.query(sql, values);
            metrics.persisted.inc(r.rowCount ?? events.length);
            return r.rowCount ?? events.length;
        } catch (err) {
            // Per-row fallback: izbegava Npgsql "0 parameters" bind grešku.
            // Sporije (N round-trip-ova) ali ne gubi batch.
            const msg = (err as Error).message;
            log.warn(
                { err: msg, batch: events.length },
                'Multi-VALUES insert failed, falling back to per-row INSERT',
            );
            return await this.writeRowsOneByOne(events, tPersist);
        }
    }

    /**
     * Per-row INSERT u transakciji. Koristi se kao fallback kad multi-VALUES
     * putanja pukne (Npgsql bind bug, edge case u pg drajveru).
     */
    private async writeRowsOneByOne(
        events: TelemetryEvent[],
        tPersist: Date,
    ): Promise<number> {
        const cols = 'device_id, pilot_index, replica, "sessionTime", "frameIdentifier", "speed", "engineTemperature", "tyresSurfaceTemperature", "worldPositionX", "worldPositionY", "worldPositionZ", t_emit, t_persist, payload';
        const sql = `INSERT INTO telemetry (${cols}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, to_timestamp($12), to_timestamp($13), $14)`;

        try {
            await this.client.query('BEGIN');
            for (const e of events) {
                const v = [
                    e.device_id,
                    e.pilot_index,
                    e.replica ?? 0,
                    e.sessionTime,
                    e.frameIdentifier,
                    e.speed,
                    e.engineTemperature,
                    e.tyresSurfaceTemperature,
                    e.worldPositionX,
                    e.worldPositionY,
                    e.worldPositionZ,
                    e.t_emit / 1000,
                    tPersist.getTime() / 1000,
                    JSON.stringify(e),
                ];
                await this.client.query(sql, v);
            }
            await this.client.query('COMMIT');
            metrics.persisted.inc(events.length);
            return events.length;
        } catch (err) {
            await this.client.query('ROLLBACK').catch(() => { /* ignore */ });
            metrics.dropped.inc(events.length);
            log.error(
                { err: (err as Error).message, batch: events.length },
                'Per-row INSERT fallback failed',
            );
            throw err;
        }
    }

    async stop(): Promise<void> {
        if (this.connected) {
            await this.client.end();
            this.connected = false;
        }
    }
}
