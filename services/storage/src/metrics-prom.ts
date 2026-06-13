/**
 * Minimalna Prometheus metrika klijentska biblioteka, dovoljna za /metrics
 * output. Implementira Counter i Gauge sa tekstualnim dumpom.
 *
 * Format je kompatibilan sa prom-client formatom (koji koristi i ingestion .NET
 * servis), tako da se rezultati mogu spojiti u jedan scrape.
 */

export class Counter {
    private value = 0;
    constructor(public readonly name: string, public readonly help: string, public readonly reg: Registry) {
        reg.register(this);
    }
    inc(n = 1) { this.value += n; }
    get(): number { return this.value; }
}

export class Gauge {
    private value = 0;
    constructor(public readonly name: string, public readonly help: string, public readonly reg: Registry) {
        reg.register(this);
    }
    set(n: number) { this.value = n; }
    inc(n = 1) { this.value += n; }
    dec(n = 1) { this.value -= n; }
    get(): number { return this.value; }
}

type Metric = Counter | Gauge;

export class Registry {
    private metrics: Metric[] = [];
    register(m: Metric) { this.metrics.push(m); }
    /** Vrati tekstualni dump u Prometheus exposition formatu. */
    dump(): string {
        const lines: string[] = [];
        for (const m of this.metrics) {
            lines.push(`# HELP ${m.name} ${m.help}`);
            lines.push(`# TYPE ${m.name} ${m instanceof Counter ? 'counter' : 'gauge'}`);
            lines.push(`${m.name} ${m.get()}`);
        }
        return lines.join('\n') + '\n';
    }
}

/** collectDefaultMetrics - simulacija. Ne uključujemo prave process metrike
 *  (zahtevaju 'pidusage' ili sl.), dovoljno nam je naš app metrics. */
export function collectDefaultMetrics(_opts: { register: Registry }): void {
    // no-op
}
