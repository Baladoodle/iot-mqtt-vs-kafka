import * as mqtt from 'mqtt';
import pino from 'pino';
import { metrics } from '../metrics';
import { RawMessage } from '../models';

const log = pino({ name: 'mqtt-consumer' });

/**
 * MQTT consumer: subscribes na topic prefix i prosleđuje poruke u callback.
 * - Subscribuje se sa QoS iz MQTT_SUB_QOS (može biti 0/1/2)
 * - cleanSession=false zadržava poruke na brokeru dok nismo reconnect-ovani
 */
export class MqttConsumer {
    private client: mqtt.MqttClient | null = null;

    constructor(
        private readonly url: string,
        private readonly port: number,
        private readonly clientId: string,
        private readonly topic: string,
        private readonly qos: 0 | 1 | 2,
        private readonly cleanSession: boolean,
        private readonly onMessage: (m: RawMessage) => Promise<void>,
    ) {}

    async start(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client = mqtt.connect({
                host: this.url,
                port: this.port,
                clientId: this.clientId,
                clean: this.cleanSession,
                reconnectPeriod: 1000,
                connectTimeout: 30_000,
                protocolVersion: 5,
            });

            const onConnect = () => {
                log.info({ url: this.url, topic: this.topic, qos: this.qos }, 'MQTT connected');
                this.client!.subscribe(this.topic, { qos: this.qos }, (err) => {
                    if (err) {
                        log.error({ err: err.message }, 'MQTT subscribe failed');
                        reject(err);
                    } else {
                        log.info({ topic: this.topic, qos: this.qos }, 'MQTT subscribed');
                        resolve();
                    }
                });
            };

            this.client.on('connect', onConnect);
            this.client.on('error', (err) => {
                log.error({ err: err.message }, 'MQTT error');
            });
            this.client.on('reconnect', () => {
                log.info('MQTT reconnecting');
            });
            this.client.on('message', (topic, payload, packet) => {
                metrics.received.inc();
                const msg: RawMessage = {
                    payload,
                    topic,
                    key: packet.properties?.userProperties?.device_id as string | undefined,
                };
                this.onMessage(msg).catch((err) => {
                    log.error({ err: err.message }, 'onMessage failed');
                });
            });
        });
    }

    async stop(): Promise<void> {
        return new Promise((resolve) => {
            if (this.client) {
                this.client.end(false, {}, () => resolve());
            } else {
                resolve();
            }
        });
    }
}
