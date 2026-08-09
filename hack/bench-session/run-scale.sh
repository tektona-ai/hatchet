#!/usr/bin/env bash
# Run the session-start benchmark against one engine, from a clean database.
#
# Works on macOS and Linux. Needs: docker compose (Postgres + NATS), the Hatchet
# binaries built from this repo, and for the Resonate side a `resonate` server
# binary built from https://github.com/resonatehq/resonate.
#
#   ./run-scale.sh hatchet capacity 1000
#   ./run-scale.sh hatchet session 32
#   ./run-scale.sh resonate capacity 4000
#
# Always start from a clean database. Runs left over from an earlier attempt are
# retried by the engine and inherited by the next worker that connects, which is
# load the measurement should not carry — and it looks exactly like the engine
# falling over.
set -euo pipefail

ENGINE=${1:?usage: run-scale.sh <hatchet|resonate> <session|capacity> <n>}
MODE=${2:?usage: run-scale.sh <hatchet|resonate> <session|capacity> <n>}
N=${3:?usage: run-scale.sh <hatchet|resonate> <session|capacity> <n>}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKDIR=${WORKDIR:-$ROOT/.bench}
RESONATE_BIN=${RESONATE_BIN:-resonate}
PG_CONTAINER=${PG_CONTAINER:-hatchet-postgres-1}
TENANT=${TENANT:-707d0855-80ab-4e1f-a156-f1c4546cbf52}

mkdir -p "$WORKDIR"

# Capacity mode parks sessions on the first wait, so the step durations only get
# in the way. Session mode uses the real ones.
if [ "$MODE" = "capacity" ]; then
  WORK=(-start-sandbox-ms 10 -clone-ms 10 -connect-ms 2000 -start-agent-ms 10 -prompt-ms 10)
  SHAPE=(-mode capacity -n "$N" -hold-seconds 45 -fill-timeout 600)
else
  WORK=()
  SHAPE=(-n "$N" -concurrency "$N" -warmup 8)
fi

rss_mb() { # rss_mb <pid> — resident size in MB, portable
  ps -o rss= -p "$1" 2>/dev/null | awk '{printf "%.0f", $1/1024}'
}

sample() { # sample <worker-pattern> <server-pid> <csv>
  local pat=$1 server=$2 csv=$3
  : > "$csv"
  while true; do
    local wpid w s
    wpid=$(pgrep -f "$pat" | head -1 || true)
    w=$([ -n "$wpid" ] && rss_mb "$wpid" || echo 0)
    s=$(rss_mb "$server")
    echo "$(date +%s),${w:-0},${s:-0}" >> "$csv"
    sleep 5
  done
}

reset_db() { # reset_db <name>
  docker exec "$PG_CONTAINER" psql -U hatchet -d postgres -q \
    -c "DROP DATABASE IF EXISTS $1 WITH (FORCE);" -c "CREATE DATABASE $1;" >/dev/null
}

case "$ENGINE" in
hatchet)
  pkill -f 'bin/hatchet-engin[e]' 2>/dev/null || true
  sleep 3
  reset_db hatchet

  cd "$ROOT"
  set -a; . .env; set +a
  go run ./cmd/hatchet-migrate >/dev/null
  go run ./cmd/hatchet-admin seed >/dev/null
  go run ./cmd/hatchet-admin token create --name bench --tenant-id "$TENANT" 2>/dev/null \
    | tail -1 > "$WORKDIR/hatchet.token"

  go run ./cmd/hatchet-engine --no-graceful-shutdown > "$WORKDIR/engine.log" 2>&1 &
  server=$!
  until nc -z 127.0.0.1 7070 2>/dev/null; do sleep 2; done

  export HATCHET_CLIENT_TOKEN="$(cat "$WORKDIR/hatchet.token")"
  export HATCHET_CLIENT_HOST_PORT=127.0.0.1:7070
  export HATCHET_CLIENT_TLS_STRATEGY=none
  export HATCHET_CLIENT_LOG_LEVEL=warn

  # Durable slots must cover every session that will wait at once. A parked
  # session holds one, and sessions past the limit queue until they time out.
  sample 'bench-session/hatchet' "$server" "$WORKDIR/$ENGINE-$MODE-$N.csv" &
  sampler=$!

  go run ./hack/bench-session/hatchet "${SHAPE[@]}" "${WORK[@]}" \
    -durable-slots $((N + 200)) -slots $((N + 200)) \
    -execution-timeout 40m -timeout 3600 \
    -label "$ENGINE-$MODE-$N" -out "$WORKDIR/results.jsonl" 2>&1 | grep -v '"level"' || true
  ;;

resonate)
  pkill -f 'resonate serv[e]' 2>/dev/null || true
  sleep 3
  reset_db resonate

  RESONATE_SERVER__PORT=8001 RESONATE_LEVEL=warn \
  RESONATE_STORAGE__TYPE=postgres \
  RESONATE_STORAGE__POSTGRES__URL="postgres://hatchet:hatchet@127.0.0.1:5431/resonate" \
  RESONATE_STORAGE__POSTGRES__POOL_SIZE=40 \
    "$RESONATE_BIN" serve > "$WORKDIR/resonate.log" 2>&1 &
  server=$!
  until curl -sf http://127.0.0.1:8001/health >/dev/null 2>&1; do sleep 2; done

  sample 'bench-session/resonate' "$server" "$WORKDIR/$ENGINE-$MODE-$N.csv" &
  sampler=$!

  cd "$ROOT/hack/bench-session/resonate"
  go run . "${SHAPE[@]}" "${WORK[@]}" -timeout 3600 \
    -label "$ENGINE-$MODE-$N" -out "$WORKDIR/results.jsonl" || true
  ;;

*)
  echo "unknown engine: $ENGINE"; exit 1 ;;
esac

kill "$sampler" 2>/dev/null || true
kill "$server" 2>/dev/null || true

awk -F, '{if ($2>w) w=$2; if ($3>s) s=$3} END {printf "memory peak: worker %d MB, server %d MB\n", w, s}' \
  "$WORKDIR/$ENGINE-$MODE-$N.csv"
