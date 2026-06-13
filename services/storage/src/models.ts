/**
 * Tip jednog telemetrijskog eventa. Isti shape kao iot-ingestion payload.
 */
export interface TelemetryEvent {
    device_id: string;
    pilot_index: number;
    replica: number;
    sessionTime: number;
    frameIdentifier: number;
    speed: number;
    engineTemperature: number;
    tyresSurfaceTemperature: number;
    worldPositionX: number;
    worldPositionY: number;
    worldPositionZ: number;
    /** Unix ms kada je ingestion publish-ovao. */
    t_emit: number;
}

/**
 * Red koji se čuva u Postgres. Ima dodatno t_persist.
 */
export interface TelemetryRow extends TelemetryEvent {
    t_persist: Date;
}

/**
 * Tip poruke koja stiže sa brokera. MQTT/Kafka vraćaju Buffer.
 */
export interface RawMessage {
    payload: Buffer;
    /** Kafka: key. MQTT: client id topic. */
    key?: string;
    topic?: string;
    partition?: number;
    offset?: string;
}
