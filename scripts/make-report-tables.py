#!/usr/bin/env python3
"""
make-report-tables.py

Pretražuje results/raw/* direktorijume, parsira logove i metrics dumpove,
i generiše agregirane CSV-ove u results/tables/.

Output tabele:
  - results/tables/scenario-A.csv  (sve throughput run-ove)
  - results/tables/scenario-B.csv  (sve disconnect run-ove)
  - results/tables/scenario-C.csv  (burst)
  - results/tables/scenario-D.csv  (E2E latency)
  - report/comparison-table.md     (auto-popunjavanje)
"""

import csv
import json
import os
import re
import sys
from pathlib import Path

# Default trajanje scenarija u sekundama (kada ingestion.log ne daje
# eksplicitnu vrednost). Poklapa se sa DURATION_S env u scenario-*.sh.
SCENARIO_DURATIONS = {
    "A": 30,    # 30s u scenario-a-throughput.sh
    "B": 120,   # 120s (60s warm-up + 30s disconnect + ~30s reconnect)
    "C": 180,   # 60s warm + 10s burst + 60s cool + buffer
    "D": 180,   # realtime replay do 180s
}

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "results" / "raw"
TABLES = ROOT / "results" / "tables"
REPORT_DIR = ROOT / "report"


def parse_prom_metrics(text: str) -> dict:
    """Vrati dict {name: value} iz Prometheus exposition formata."""
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            name, val = parts
            try:
                out[name] = float(val)
            except ValueError:
                pass
    return out


def parse_ingest_log(log_path: Path) -> dict:
    """Iz ingestion.log izvuci 'emitted' i 'dropped' iz završne linije.

    Pokriva dva formata:
      - 'Rate mode završen: emitted=N/M'
      - 'Ingestion završen. emitted=N dropped=M' (Program.cs)
      - 'Realtime mode završen: emitted=N' (Scheduler.cs)
    """
    info = {"emitted": None, "dropped": None, "duration_s": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    # Prvo probaj "Ingestion završen. emitted=N dropped=M"
    m = re.search(r"emitted=(\d+)\s+dropped=(\d+)", text)
    if m:
        info["emitted"] = int(m.group(1))
        info["dropped"] = int(m.group(2))
    else:
        # Probaj "Rate/Realtime mode završen: emitted=N" (bez dropped)
        m = re.search(r"mode završen:\s+emitted=(\d+)", text)
        if m:
            info["emitted"] = int(m.group(1))
    return info


def parse_storage_log(log_path: Path) -> dict:
    """Iz storage.log izvuci batch stats i upis u DB."""
    info = {"batches": None, "received": None, "persisted": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    for name, key in (("batches", "batches"), ("received", "received"), ("persisted", "persisted")):
        m = re.search(rf"^{name}[^=]*=\s*(\d+)", text, re.MULTILINE)
        if m:
            info[key] = int(m.group(1))
    return info


def parse_analytics_log(log_path: Path) -> dict:
    """Broji ALERT linije i poslednji mean.

    Serilog format je "[HH:MM:SS WRN] ALERT window_start=..." pa NE
    koristimo ^ anchor (koji bi tražio ALERT na početku reda) već
    tražimo 'ALERT window_start=' substring.
    """
    info = {"alerts": 0, "last_mean": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    info["alerts"] = len(re.findall(r"ALERT window_start=", text))
    matches = re.findall(r"mean_engine_temp=([\d.]+)", text)
    if matches:
        info["last_mean"] = float(matches[-1])
    return info


def parse_timing_log(log_path: Path) -> dict:
    """Iz timing.log izvuci recovery, ALERT-after-reconnect, E2E latenciju.

    Polja:
      - recovery_s: 'Recovery detected at +Ns after reconnect ...'
      - first_alert_after_reconnect: 'First ALERT after reconnect: <text>'
      - e2e_latency_s: 'E2E latency (approx): Ns'
      - peak_backlog: max(received - persisted) iz *_metrics_*.txt fajlova
    """
    info = {"recovery_s": None, "first_alert_after_reconnect": None, "e2e_latency_s": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    m = re.search(r"Recovery detected at \+(\d+)s", text)
    if m:
        info["recovery_s"] = int(m.group(1))
    m = re.search(r"No recovery within 60s", text)
    if m and info["recovery_s"] is None:
        info["recovery_s"] = "timeout"
    # Podržavamo i novi format "(inject→alert)" i stari "(approx)"
    m = re.search(r"E2E latency \([^)]+\):\s+(-?\d+)s", text)
    if m:
        info["e2e_latency_s"] = int(m.group(1))
    return info


def parse_storage_metrics_snapshots(run_dir: Path) -> dict:
    """Pronađi sve storage_metrics_*.txt fajlove i izračunaj peak_backlog.

    Za scenario C očekujemo:
      - storage_metrics.txt               (krajnje stanje, cooldown_end)
      - storage_metrics_burst_end.txt     (odmah posle burst-a)
      - storage_metrics_cooldown_mid.txt  (sredina cooldown-a)
    Za scenario B i D očekujemo samo storage_metrics.txt.
    """
    out = {"peak_backlog": None, "snapshots": []}
    for p in sorted(run_dir.glob("storage_metrics*.txt")):
        text = p.read_text(errors="ignore")
        received = None
        persisted = None
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("storage_received_total "):
                try: received = int(float(line.rsplit(" ", 1)[1]))
                except (ValueError, IndexError): pass
            elif line.startswith("storage_persisted_total "):
                try: persisted = int(float(line.rsplit(" ", 1)[1]))
                except (ValueError, IndexError): pass
        if received is not None and persisted is not None:
            out["snapshots"].append({"file": p.name, "received": received, "persisted": persisted})
    if out["snapshots"]:
        out["peak_backlog"] = max(s["received"] - s["persisted"] for s in out["snapshots"])
    return out


def parse_stats_csv(stats_path: Path) -> dict:
    """Sažmi docker stats CSV po kontejneru — prosečni CPU i RAM.

    Filtrira samo projektne kontejnere (`iots-proj2-*`) da strani procesi
    na hostu (npr. litellm, claude-code) ne kontaminiraju ukupne metrike.
    """
    PROJECT_PREFIX = "iots-proj2-"
    by_container = {}
    if not stats_path.exists():
        return by_container
    with stats_path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            name = row.get("Name") or row.get("Container") or ""
            if not name or not name.startswith(PROJECT_PREFIX):
                continue
            cpu = row.get("CPUPerc", "0%").replace("%", "")
            mem = row.get("MemUsage", "0MiB / 0MiB")
            cpu = float(cpu) if cpu else 0.0
            used = mem.split("/")[0].strip() if "/" in mem else mem
            mb = 0.0
            for unit, mult in (("GiB", 1024), ("MiB", 1), ("KiB", 0.001), ("B", 1e-6)):
                if unit in used:
                    try:
                        mb = float(used.replace(unit, "").strip()) * mult
                    except ValueError:
                        mb = 0.0
                    break
            d = by_container.setdefault(name, {"cpu": [], "mem_mb": []})
            d["cpu"].append(cpu)
            d["mem_mb"].append(mb)
    # Proseci
    summary = {}
    for name, d in by_container.items():
        if d["cpu"]:
            summary[name] = {
                "cpu_avg": sum(d["cpu"]) / len(d["cpu"]),
                "mem_mb_avg": sum(d["mem_mb"]) / len(d["mem_mb"]),
            }
    return summary


def main():
    if not RAW.exists():
        print(f"Nema results/raw; preskačem.", file=sys.stderr)
        return
    TABLES.mkdir(parents=True, exist_ok=True)

    scenarios = {
        "A": [],
        "B": [],
        "C": [],
        "D": [],
    }

    for run_dir in sorted(RAW.iterdir()):
        if not run_dir.is_dir():
            continue
        name = run_dir.name
        # Format dirnames:
        #   scenario-A-mqtt-100-qos1          (A: num + level)
        #   scenario-A-kafka-1000-acksall     (A: num + level)
        #   scenario-B-mqtt-20260614-152243   (B/C/D: timestamp)
        #   scenario-C-kafka-20260614-152503  (B/C/D: timestamp)
        # Regex ima OBA opcionalna segmenta i prepušta Pythonu da odluči
        # koji je prazan.
        m = re.match(r"scenario-([A-D])-(mqtt|kafka)(?:-(\d{8}-\d{6}))?(?:-(\d+))?(?:-([a-zA-Z0-9]+))?", name)
        if not m:
            continue
        scenario, broker, timestamp, n, level = m.groups()
        run_info = {
            "run_id": name,
            "broker": broker,
            "num_devices": n or "",
            "level": level or "",
        }
        # Parsiraj ingestion log
        run_info.update(parse_ingest_log(run_dir / "ingestion.log"))
        # Analytics
        run_info.update(parse_analytics_log(run_dir / "analytics.log"))
        # Timing log (recovery, E2E latency)
        run_info.update(parse_timing_log(run_dir / "timing.log"))
        # Storage: prefer metrics file (has exact counters), fall back to log
        storage_metrics = run_dir / "storage_metrics.txt"
        if storage_metrics.exists():
            sm = parse_prom_metrics(storage_metrics.read_text())
            run_info["received"] = int(sm.get("storage_received_total", 0))
            run_info["persisted"] = int(sm.get("storage_persisted_total", 0))
            run_info["dropped"] = int(sm.get("storage_dropped_total", 0))
            run_info["batches"] = int(sm.get("storage_batches_total", 0))
            # Lag i p95 lag (čuvamo kao float, ne int)
            run_info["storage_lag_ms"] = sm.get("storage_lag_ms", None)
            run_info["storage_p95_lag_ms"] = sm.get("storage_p95_lag_ms", None)
        else:
            run_info.update(parse_storage_log(run_dir / "storage.log"))
            run_info["storage_lag_ms"] = None
            run_info["storage_p95_lag_ms"] = None
        # Peak backlog iz svih storage_metrics snapshot-ova (scenario C)
        snapshots = parse_storage_metrics_snapshots(run_dir)
        if snapshots.get("peak_backlog") is not None:
            run_info["peak_backlog"] = snapshots["peak_backlog"]
        # Stats
        stats = parse_stats_csv(run_dir / "stats.csv")
        # Ukupan CPU i RAM
        total_cpu = sum(s["cpu_avg"] for s in stats.values())
        total_mem = sum(s["mem_mb_avg"] for s in stats.values())
        run_info["total_cpu_avg"] = round(total_cpu, 2)
        run_info["total_mem_mb_avg"] = round(total_mem, 2)
        # Metrike servisa
        for svc, fname in (("ingest", "ingest_metrics.txt"),
                            ("storage", "storage_metrics.txt"),
                            ("analytics", "analytics_metrics.txt")):
            f = run_dir / fname
            if f.exists():
                m = parse_prom_metrics(f.read_text())
                run_info[f"{svc}_emitted"] = m.get("ingest_emitted_total", m.get("storage_received_total", 0))
                run_info[f"{svc}_throughput"] = m.get("ingest_throughput_msg_per_sec", 0)
        # Throughput (msg/s) — duration po scenariju
        if run_info.get("emitted") and run_info.get("duration_s") is None:
            run_info["duration_s"] = SCENARIO_DURATIONS.get(scenario, 30)
        if run_info.get("emitted") and run_info.get("duration_s"):
            run_info["throughput_msg_per_sec"] = round(run_info["emitted"] / run_info["duration_s"], 1)
        # Gubitak
        if run_info.get("emitted") and run_info.get("persisted") is not None:
            sent = run_info["emitted"]
            rec = run_info["persisted"]
            run_info["loss_pct"] = round(100.0 * (sent - rec) / sent, 2) if sent else 0.0
        scenarios[scenario].append(run_info)

    # Snimi CSV po scenariju
    for s, rows in scenarios.items():
        if not rows:
            continue
        out = TABLES / f"scenario-{s}.csv"
        keys = sorted({k for r in rows for k in r.keys()})
        with out.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)
        print(f"[ok] {out} ({len(rows)} redova)")

    # Za comparison-table.md, koristimo SAMO najskoriji run po (scenario, broker)
    # paru. Stari runovi ostaju u CSV (za forenziku), ali u tabeli prikazujemo
    # samo reprezentativni najnoviji rezultat.
    def latest_per_broker(rows):
        # run_id je 'scenario-X-{broker}-{...}'. Sortiranje po run_id = hronološki.
        latest = {}
        for r in rows:
            b = r["broker"]
            if b not in latest or r["run_id"] > latest[b]["run_id"]:
                latest[b] = r
        return list(latest.values())

    scenarios_latest = {s: latest_per_broker(rows) for s, rows in scenarios.items()}

    # Generiši comparison-table.md
    md = REPORT_DIR / "comparison-table.md"
    lines = ["# Uporedna tabela performansi — auto-generisano\n"]
    lines.append("Stupci: Throughput (msg/s), gubitak (%), prosečni CPU (%), prosečni RAM (MB), alerti.\n")
    lines.append("\n## Scenario A — Throughput po uređajima i QoS/ACKS\n")
    lines.append("| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg (%) | RAM avg (MB) |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in scenarios_latest["A"]:
        lines.append(
            f"| {r['broker']} | {r['num_devices']} | {r['level']} | "
            f"{r.get('throughput_msg_per_sec', 'n/a')} | {r.get('loss_pct', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    lines.append("\n## Scenario B — Disconnect / Recovery\n")
    lines.append("| Broker | Uređaji | Trajanje (s) | Recovery (s) | E2E (s) | ALERT nakon reconnect | CPU avg (%) | RAM avg (MB) |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in scenarios_latest["B"]:
        lines.append(
            f"| {r['broker']} | n/a | {r.get('duration_s', 'n/a')} | "
            f"{r.get('recovery_s', 'n/a')} | "
            f"{r.get('e2e_latency_s', 'n/a')} | "
            f"{r.get('first_alert_after_reconnect', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    lines.append("\n## Scenario C — Burst / Backlog\n")
    lines.append("| Broker | Uređaji | Trajanje (s) | Lag (ms) | p95 lag (ms) | Peak backlog | CPU avg (%) | RAM avg (MB) |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in scenarios_latest["C"]:
        # Formatuj lag kao int (bez .0) kad je ceo broj
        lag = r.get("storage_lag_ms")
        p95 = r.get("storage_p95_lag_ms")
        lag_str = f"{int(lag)}" if lag is not None and lag == int(lag) else (f"{lag}" if lag is not None else "n/a")
        p95_str = f"{int(p95)}" if p95 is not None and p95 == int(p95) else (f"{p95}" if p95 is not None else "n/a")
        lines.append(
            f"| {r['broker']} | {r.get('num_devices') or 'n/a'} | {r.get('duration_s', 'n/a')} | "
            f"{lag_str} | {p95_str} | "
            f"{r.get('peak_backlog', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    lines.append("\n## Scenario D — E2E latencija (alert)\n")
    lines.append("| Broker | Alerts | last_mean | E2E (s) | CPU avg (%) | RAM avg (MB) |")
    lines.append("|---|---|---|---|---|---|")
    for r in scenarios_latest["D"]:
        lines.append(
            f"| {r['broker']} | {r.get('alerts', 0)} | {r.get('last_mean', 'n/a')} | "
            f"{r.get('e2e_latency_s', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    md.write_text("\n".join(lines) + "\n")
    print(f"[ok] {md}")


if __name__ == "__main__":
    main()
