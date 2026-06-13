import { TelemetryEvent } from '../models';
import { metrics } from '../metrics';

/**
 * Blackhole sink — samo broji. Koristi se za Scenarije A i C da DB
 * ne bude usko grlo (spec: "implementirati batching ili privremeno
 * isključiti upis u bazu").
 */
export class BlackholeSink {
    async writeBatch(events: TelemetryEvent[]): Promise<number> {
        metrics.persisted.inc(events.length);
        return events.length;
    }
}
