import pino from 'pino';
import { TelemetryEvent } from './models';
import { metrics } from './metrics';

const log = pino({ name: 'batcher' });

export interface BatcherSink {
    writeBatch(events: TelemetryEvent[]): Promise<number>;
}

/**
 * Batcher: koalescira poruke u batcheve od BATCH_SIZE ili FLUSH_INTERVAL_MS,
 * koje god prvo istekne. Backpressure: ako je writeBatch spor, akumulacija
 * može narasti — exposed kroz lastBatchSize metriku.
 */
export class Batcher {
    private buffer: TelemetryEvent[] = [];
    private timer: NodeJS.Timeout | null = null;
    private flushing = false;
    private readonly batchSize: number;
    private readonly flushIntervalMs: number;
    private readonly sink: BatcherSink;
    private readonly onFlushComplete?: (batch: TelemetryEvent[], lagMs: number) => void;
    private lagHistory: number[] = [];

    constructor(opts: {
        batchSize: number;
        flushIntervalMs: number;
        sink: BatcherSink;
        onFlushComplete?: (batch: TelemetryEvent[], lagMs: number) => void;
    }) {
        this.batchSize = opts.batchSize;
        this.flushIntervalMs = opts.flushIntervalMs;
        this.sink = opts.sink;
        this.onFlushComplete = opts.onFlushComplete;
    }

    start(): void {
        if (this.timer) return;
        this.timer = setInterval(() => {
            this.flush().catch((err) => log.error({ err: err.message }, 'Periodic flush failed'));
        }, this.flushIntervalMs);
    }

    /** Dodaj event. Ako buffer dosegne batchSize, odmah flush. */
    add(event: TelemetryEvent): void {
        this.buffer.push(event);
        if (this.buffer.length >= this.batchSize) {
            this.flush().catch((err) => log.error({ err: err.message }, 'Size-triggered flush failed'));
        }
    }

    size(): number {
        return this.buffer.length;
    }

    async flush(): Promise<void> {
        if (this.flushing || this.buffer.length === 0) return;
        this.flushing = true;
        const batch = this.buffer;
        this.buffer = [];

        const t0 = Date.now();
        let lagMs = 0;
        try {
            await this.sink.writeBatch(batch);
        } catch (err) {
            log.error({ err: (err as Error).message, batch: batch.length }, 'Sink write failed');
        } finally {
            const t1 = Date.now();
            lagMs = t1 - t0;
            // Prosečan lag batcha: razlika između t_persist poslednje poruke
            // i t_emit. Pojedinačno nemamo, koristimo batch vreme.
            this.lagHistory.push(lagMs);
            if (this.lagHistory.length > 1000) this.lagHistory.shift();
            const p95 = this.percentile(this.lagHistory, 0.95);
            metrics.lastBatchSize.set(batch.length);
            metrics.lastLagMs.set(lagMs);
            metrics.p95LagMs.set(p95);
            metrics.batches.inc();
            if (this.onFlushComplete) this.onFlushComplete(batch, lagMs);
            this.flushing = false;
        }
    }

    private percentile(arr: number[], p: number): number {
        if (arr.length === 0) return 0;
        const sorted = [...arr].sort((a, b) => a - b);
        const idx = Math.floor(sorted.length * p);
        return sorted[Math.min(idx, sorted.length - 1)];
    }

    async stop(): Promise<void> {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        await this.flush();
    }
}
