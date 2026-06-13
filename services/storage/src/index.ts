import * as http from 'http';
import pino from 'pino';
import { Batcher } from './batcher';
import { BlackholeSink } from './sink/blackhole';
import { PostgresSink } from './sink/postgres';
import { MqttConsumer } from './brokers/mqtt';
import { KafkaStorageConsumer } from './brokers/kafka';
import { TelemetryEvent, RawMessage } from './models';
import { metrics } from './metrics';

const log = pino({ name: 'storage' });

// --- env ---
const BROKER = process.env.BROKER ?? 'mqtt';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '500', 10);
const FLUSH_INTERVAL_MS = parseInt(process.env.FLUSH_INTERVAL_MS ?? '200', 10);
const DB_ENABLED = (process.env.DB_ENABLED ?? 'true') === 'true';
const DB_URL = process.env.DATABASE_URL ?? 'postgres://iot:iot@postgres:5432/iot';
const METRICS_PORT = parseInt(process.env.METRICS_PORT ?? '9092', 10);

// MQTT env
const MQTT_URL = process.env.MQTT_URL ?? 'mosquitto';
const MQTT_PORT = parseInt(process.env.MQTT_PORT ?? '1883', 10);
const MQTT_CLIENT_ID = process.env.MQTT_CLIENT_ID ?? `storage-${process.pid}`;
const MQTT_TOPIC = process.env.MQTT_TOPIC ?? 'iot/telemetry/#';
const MQTT_QOS = (parseInt(process.env.MQTT_SUB_QOS ?? process.env.MQTT_QOS ?? '1', 10) as 0 | 1 | 2);
const MQTT_CLEAN_SESSION = (process.env.MQTT_CLEAN_SESSION ?? 'true') === 'true';

// Kafka env
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'kafka:9092').split(',');
const KAFKA_TOPIC = process.env.KAFKA_TOPIC ?? 'iot-telemetry';
const KAFKA_GROUP = process.env.KAFKA_GROUP ?? 'storage-cg';

async function main() {
    log.info({ BROKER, BATCH_SIZE, FLUSH_INTERVAL_MS, DB_ENABLED }, 'Storage starting');

    // --- Sink ---
    const sink = DB_ENABLED ? new PostgresSink(DB_URL) : new BlackholeSink();

    if (DB_ENABLED) {
        await (sink as PostgresSink).start();
    } else {
        log.warn('DB_ENABLED=false — blackhole sink (za Scenarije A i C)');
    }

    // --- Batcher ---
    const batcher = new Batcher({
        batchSize: BATCH_SIZE,
        flushIntervalMs: FLUSH_INTERVAL_MS,
        sink,
    });
    batcher.start();

    // --- Consumer ---
    let consumer: MqttConsumer | KafkaStorageConsumer;
    if (BROKER === 'mqtt') {
        consumer = new MqttConsumer(
            MQTT_URL, MQTT_PORT, MQTT_CLIENT_ID, MQTT_TOPIC, MQTT_QOS, MQTT_CLEAN_SESSION,
            async (msg: RawMessage) => {
                const evt = parseEvent(msg);
                if (evt) batcher.add(evt);
            },
        );
        await (consumer as MqttConsumer).start();
    } else {
        const kc = new KafkaStorageConsumer(KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_GROUP, async (msg) => {
            const evt = parseEvent(msg);
            if (evt) batcher.add(evt);
        });
        consumer = kc;
        await kc.start();
    }

    // --- /metrics HTTP ---
    const server = http.createServer((req, res) => {
        if (req.url === '/metrics') {
            res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
            res.end(metrics.register.dump());
        } else if (req.url === '/healthz') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'ok', broker: BROKER, batcherSize: batcher.size() }));
        } else {
            res.writeHead(404);
            res.end();
        }
    });
    server.listen(METRICS_PORT, () => log.info({ port: METRICS_PORT }, 'metrics server listening'));

    // --- graceful shutdown ---
    const shutdown = async () => {
        log.info('Shutting down...');
        server.close();
        await batcher.stop();
        if (DB_ENABLED) await (sink as PostgresSink).stop();
        if (BROKER === 'mqtt') {
            await (consumer as MqttConsumer).stop();
        } else {
            await (consumer as KafkaStorageConsumer).stop();
        }
        process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}

function parseEvent(msg: RawMessage): TelemetryEvent | null {
    try {
        const str = msg.payload.toString('utf-8');
        const evt = JSON.parse(str) as TelemetryEvent;
        if (typeof evt.t_emit !== 'number' || typeof evt.device_id !== 'string') {
            log.warn({ payload: str.slice(0, 200) }, 'Event missing required fields');
            return null;
        }
        return evt;
    } catch (err) {
        log.error({ err: (err as Error).message }, 'JSON parse failed');
        return null;
    }
}

main().catch((err) => {
    log.error({ err: err.message, stack: err.stack }, 'Fatal');
    process.exit(1);
});
