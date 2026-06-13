import { Kafka, Consumer, KafkaMessage, logLevel } from 'kafkajs';
import pino from 'pino';
import { metrics } from '../metrics';
import { RawMessage } from '../models';

const log = pino({ name: 'kafka-consumer' });

/**
 * Kafka consumer: svaka poruka ide u callback. Manual offset commit
 * posle batch flush-a (tako gubitak ostaje u Kafka logu ako se Storage
 * sruši pre upisa).
 */
export class KafkaStorageConsumer {
    private consumer: Consumer;
    private connected = false;

    constructor(
        private readonly brokers: string[],
        private readonly topic: string,
        private readonly groupId: string,
        private readonly onMessage: (m: RawMessage) => Promise<void>,
    ) {
        const kafka = new Kafka({
            clientId: 'iot-storage',
            brokers,
            logLevel: logLevel.WARN,
            connectionTimeout: 10_000,
        });
        this.consumer = kafka.consumer({
            groupId,
            sessionTimeout: 30_000,
            heartbeatInterval: 3_000,
        });
    }

    async start(): Promise<void> {
        await this.consumer.connect();
        await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });
        this.connected = true;
        log.info({ topic: this.topic, groupId: this.groupId }, 'Kafka consumer subscribed');

        await this.consumer.run({
            autoCommit: false,
            eachMessage: async ({ topic, partition, message }) => {
                metrics.received.inc();
                const msg: RawMessage = {
                    payload: message.value as Buffer,
                    key: message.key?.toString(),
                    topic,
                    partition,
                    offset: message.offset,
                };
                await this.onMessage(msg);
            },
        });
    }

    async commit(partition: number, offset: string): Promise<void> {
        await this.consumer.commitOffsets([
            { topic: this.topic, partition, offset: (BigInt(offset) + 1n).toString() },
        ]);
    }

    async stop(): Promise<void> {
        if (this.connected) {
            await this.consumer.disconnect();
            this.connected = false;
        }
    }
}
