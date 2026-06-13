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
    """Iz ingestion.log izvuci 'emitted' i 'dropped' iz završne linije."""
    info = {"emitted": None, "dropped": None, "duration_s": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    m = re.search(r"emitted=(\d+)\s+dropped=(\d+)", text)
    if m:
        info["emitted"] = int(m.group(1))
        info["dropped"] = int(m.group(2))
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
    """Broji ALERT linije i poslednji mean."""
    info = {"alerts": 0, "last_mean": None}
    if not log_path.exists():
        return info
    text = log_path.read_text(errors="ignore")
    info["alerts"] = len(re.findall(r"^ALERT ", text, re.MULTILINE))
    matches = re.findall(r"mean_engine_temp=([\d.]+)", text)
    if matches:
        info["last_mean"] = float(matches[-1])
    return info


def parse_stats_csv(stats_path: Path) -> dict:
    """Sažmi docker stats CSV po kontejneru — prosečni CPU i RAM."""
    by_container = {}
    if not stats_path.exists():
        return by_container
    with stats_path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            name = row.get("Name") or row.get("Container") or ""
            if not name:
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
        m = re.match(r"scenario-([A-D])-(mqtt|kafka)(?:-(\d+))?(?:-([a-zA-Z0-9]+))?", name)
        if not m:
            continue
        scenario, broker, n, level = m.groups()
        run_info = {
            "run_id": name,
            "broker": broker,
            "num_devices": n or "",
            "level": level or "",
        }
        # Parsiraj ingestion log
        run_info.update(parse_ingest_log(run_dir / "ingestion.log"))
        # Storage
        run_info.update(parse_storage_log(run_dir / "storage.log"))
        # Analytics
        run_info.update(parse_analytics_log(run_dir / "analytics.log"))
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
        # Throughput (msg/s)
        if run_info.get("emitted") and run_info.get("duration_s") is None:
            # duration je približno jednak DURATION_S env (30s default za A)
            run_info["duration_s"] = 30
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

    # Generiši comparison-table.md
    md = REPORT_DIR / "comparison-table.md"
    lines = ["# Uporedna tabela performansi — auto-generisano\n"]
    lines.append("Stupci: Throughput (msg/s), gubitak (%), prosečni CPU (%), prosečni RAM (MB), alerti.\n")
    lines.append("\n## Scenario A — Throughput po uređajima i QoS/ACKS\n")
    lines.append("| Broker | Uređaji | QoS/ACKS | Throughput (msg/s) | Gubitak (%) | CPU avg (%) | RAM avg (MB) |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in scenarios["A"]:
        lines.append(
            f"| {r['broker']} | {r['num_devices']} | {r['level']} | "
            f"{r.get('throughput_msg_per_sec', 'n/a')} | {r.get('loss_pct', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    lines.append("\n## Scenario D — E2E latencija\n")
    lines.append("| Broker | Alerts | last_mean | CPU avg | RAM avg |")
    lines.append("|---|---|---|---|---|")
    for r in scenarios["D"]:
        lines.append(
            f"| {r['broker']} | {r.get('alerts', 0)} | {r.get('last_mean', 'n/a')} | "
            f"{r.get('total_cpu_avg', 'n/a')} | {r.get('total_mem_mb_avg', 'n/a')} |"
        )
    md.write_text("\n".join(lines) + "\n")
    print(f"[ok] {md}")


if __name__ == "__main__":
    main()
