#!/usr/bin/env bash
# preflight.sh — proverava pre pokretanja da su Data.csv, .env, docker,
# docker compose, portovi slobodni.

set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

GREEN="\033[0;32m"
RED="\033[0;31m"
YEL="\033[0;33m"
NC="\033[0m"

fail=0

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YEL}!${NC} $1"; }
err()  { echo -e "  ${RED}✗${NC} $1"; fail=$((fail + 1)); }

echo "=== Proj-2 preflight ==="

# Data.csv
if [ -f "$ROOT/Data.csv" ]; then
    sz=$(du -h "$ROOT/Data.csv" | cut -f1)
    ok "Data.csv postoji ($sz)"
else
    err "Data.csv ne postoji u $ROOT"
fi

if [ -f "$ROOT/Data.full.csv" ]; then
    sz=$(du -h "$ROOT/Data.full.csv" | cut -f1)
    warn "Data.full.csv postoji ($sz) — zauzima mesto ali nije u git-u"
fi

# .env
if [ -f "$ROOT/.env" ]; then
    ok ".env postoji"
else
    warn ".env ne postoji (koristiće se .env.example vrednosti)"
fi

# Docker
if command -v docker >/dev/null 2>&1; then
    ok "docker: $(docker --version | head -c 60)"
else
    err "docker nije instaliran"
fi

if docker compose version >/dev/null 2>&1; then
    ok "docker compose: $(docker compose version | head -c 60)"
else
    err "docker compose plugin nije instaliran"
fi

# Portovi
for p in 1883 5432 9090 9091 9092; do
    if (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null; then
        err "port $p je zauzet"
    else
        ok "port $p slobodan"
    fi
done

# Compose fajlovi
for f in compose/compose.yaml compose/compose.mqtt.yaml compose/compose.kafka.yaml; do
    if [ -f "$ROOT/$f" ]; then
        ok "$f"
    else
        err "$f nedostaje"
    fi
done

if [ $fail -gt 0 ]; then
    echo
    err "Preflight failed: $fail grešaka"
    exit 1
fi
echo
ok "Preflight OK"
