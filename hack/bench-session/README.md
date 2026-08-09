# Session-start benchmark: Hatchet vs Resonate

Two harnesses that run the same emulated Tektona session start against two
durable execution engines, so the engine's share of that latency can be measured.

## What it emulates

The sequence a thread runs when a session begins:

| # | Step | Kind | Default |
|---|---|---|---|
| 1 | Start the sandbox | durable step | 50 ms |
| 2 | Wait for it to report running | wait for a signal | 790 ms |
| 3 | Clone the repositories | durable step | 3000 ms |
| 4 | Wait for the agent runtime to connect | wait for a signal | 1000 ms |
| 5 | Start the coding agent session | durable step | 500 ms |
| 6 | Send the prompt | durable step | 100 ms |

Every step sleeps instead of doing the real thing, so the gap between the measured
session time and the 5440 ms those durations add up to is what the engine costs.

The boot default comes from our own measurement — `docs-internal/benchmarks/2026-04-07-tektona-benchmark.md`
puts create-to-running at 789 ms p50. Clone, connect, agent start and prompt are
estimates; every one is a flag, so put real numbers in when you have them.

Steps 2 and 4 are the point of the exercise. Both are waits on something outside
the workflow, so a sandbox controller runs alongside the worker, waits the time
the real thing would take, and sends the signal. The signal carries the time it
was sent, and the step after the wait reports the gap — that is the engine's cost
to wake a parked workflow.

| | how the workflow waits | how the controller signals |
|---|---|---|
| Hatchet | `ctx.WaitForEvent(key, "input.session_id == '…'")` | `Events().Push(ctx, key, payload)` |
| Resonate | `ctx.Promise()` | `Promises().Resolve(ctx, "<run id>.<n>", payload)` |

That difference is not cosmetic. Hatchet addresses the waiter by event key and a
filter over the payload, so a signaller needs to know only the session ID.
Resonate addresses it by promise ID, and the ID is positional — `<run id>.<n>`
counting every `ctx.Run`, `ctx.Sleep` and `ctx.Promise` in the body in order. The
harness therefore hard-codes that the two waits are children 2 and 4. Insert a
step above them and the controller signals the wrong promise.

## Measure the wake inside a step, not in the workflow body

Both harnesses compute the wake gap in the body of the step that follows the wait,
never in the workflow body. In Resonate the workflow body runs again on every
replay, so `time.Since(sentAt)` in the body is recomputed against a later clock
each time, and the value that survives is from the last replay — which turns the
measurement into "time from the signal until the workflow finished". The first
version of this harness had exactly that bug, and it reported a 4.2 s wake for
what is really under 100 ms.

## Run it

Start Postgres and NATS, then the engine, and export a client token:

```sh
docker compose up -d postgres nats
go run ./cmd/hatchet-migrate
go run ./cmd/hatchet-admin seed
export HATCHET_CLIENT_TOKEN=...
export HATCHET_CLIENT_HOST_PORT=127.0.0.1:7070
export HATCHET_CLIENT_TLS_STRATEGY=none

go run ./hack/bench-session/hatchet -n 48 -concurrency 8 -out session-results.jsonl
```

For Resonate, build the server from https://github.com/resonatehq/resonate:

```sh
RESONATE_STORAGE__TYPE=postgres \
RESONATE_STORAGE__POSTGRES__URL=postgres://hatchet:hatchet@127.0.0.1:5431/resonate \
  resonate serve

cd hack/bench-session/resonate
go run . -n 48 -concurrency 8 -out ../../../session-results.jsonl
```

Run one engine at a time, and start Resonate on a fresh database: promises left
pending by an earlier run are redelivered to whatever worker connects next, which
is load the measurement should not carry.

## Results

One run, 2026-08-09, on a 4 vCPU / 15 GiB Linux box, both engines against the same
PostgreSQL 15.6, one engine running at a time, two repeats per cell. Hatchet used
the NATS JetStream queue backend. Emulated work totals 5440 ms.

Engine cost on top of that work, p50, both repeats:

| session starts in flight | Hatchet | Resonate |
|---|---|---|
| 1 | 243 / 216 ms | 304 / 310 ms |
| 8 | 321 / 283 ms | 316 / 300 ms |
| 32 | 1126 / 938 ms | 430 / 432 ms |

Waking a parked workflow — signal sent until the next step body starts, p50 / p95:

| session starts in flight | Hatchet | Resonate |
|---|---|---|
| 1 | 47–53 / 50–82 ms | 87–113 / 91–121 ms |
| 8 | 45–57 / 63–292 ms | 87–115 / 93–146 ms |
| 32 | 74–104 / 323–429 ms | 101–149 / 126–173 ms |

Session starts per second: 0.18 vs 0.17 at 1 in flight, 1.37 vs 1.39 at 8,
4.7 vs 5.45 at 32.

Three things this says.

**The engine is 4 to 8 percent of a session start.** Against 5.4 s of sandbox
boot, cloning and connecting, both engines cost a fraction of a second. Neither
choice moves session-start latency in a way a user would notice.

**Hatchet wakes a parked workflow about twice as fast** — around 50 ms against
around 100 ms — which is the operation this workload does most.

**Resonate holds its shape under load and Hatchet does not.** At 32 in flight
Hatchet's cost quadruples and its tail spreads (p50 6566 ms, max 7987 ms) while
Resonate stays flat (p50 5870 ms, max 5883 ms). Four cores running the engine,
the worker and Postgres together is too few for Hatchet, so read this as a
symptom of the box, not of the design — but it is the one number worth re-running
on real hardware before deciding.

## Running it yourself

`run-scale.sh` does the whole cycle for one engine — reset the database, start
it, run one cell, sample memory, print the result. It works on macOS and Linux.

```sh
docker compose up -d postgres nats

./hack/bench-session/run-scale.sh hatchet  capacity 1000
./hack/bench-session/run-scale.sh resonate capacity 4000
./hack/bench-session/run-scale.sh hatchet  session  32
```

The Resonate side needs a `resonate` server binary built from
https://github.com/resonatehq/resonate; point `RESONATE_BIN` at it.

Two rules the numbers depend on:

- **Start from a clean database every time.** Runs left over from an earlier
  attempt are retried by the engine and inherited by the next worker that
  connects. That load is invisible in the results and looks exactly like the
  engine falling over — it cost hours here before it was spotted.
- **One engine at a time.** Both do background work when idle.

## Capacity: how many sessions can be parked at once

`-mode capacity` starts N sessions at once, waits for every one to park on the
first wait, holds them there, then releases them all together. It answers the
question the latency run does not: how many sessions can be waiting, and what
does it cost to wake them.

Same box, 16 durable slots on the Hatchet worker, a default Resonate worker,
20 second hold:

| engine | sessions | parked | fill | drain | wake p50 / max | failed |
|---|---:|---:|---:|---:|---:|---:|
| Hatchet, no eviction | 64 | **13** | 300 s (gave up) | 2.0 s | 245 / 347 ms | **51** |
| Hatchet, eviction 3 s | 64 | 64 | 16.0 s | 8.9 s | 4592 / 7648 ms | 0 |
| Hatchet, eviction 3 s | 256 | 256 | 77.0 s | 25.3 s | 12744 / 24185 ms | 0 |
| Resonate | 64 | 64 | 0.5 s | 1.8 s | 179 / 340 ms | 0 |
| Resonate | 256 | 256 | 0.8 s | 3.7 s | 859 / 1234 ms | 0 |

**A parked Hatchet session holds a durable slot.** With eviction off, 16 slots
held 13 sessions and the other 51 queued behind them until they timed out. That
ceiling is configuration, not a hard limit — size the fleet's durable slots to
peak parked sessions and it goes away. What this run does not tell you is what a
held slot costs the worker in memory, which is the number you need before setting
that figure to thousands.

**Eviction lifts the ceiling and costs seconds to come back.** With a 3 second
eviction TTL, 256 sessions parked on a 16-slot worker. Waking them cost 12.7 s at
p50 and 24 s at the tail, against 0.9 s and 1.2 s for Resonate.

**Resonate parks for almost nothing.** The workflow suspends, the task goes back
to the server, the worker holds no state. 256 parked in 0.8 s, and filling scales
nearly flat where Hatchet's is linear at roughly 0.3 s per session.

Read the absolute times with the box in mind — four cores running the engine, the
worker and Postgres together starves Hatchet. The structural difference does not
come from the box: Hatchet parks in a worker slot and restores an evicted run
through the queue, Resonate parks in the server.

### Correction to the table above

Those eviction rows were measured on a database still holding runs from earlier
attempts, so read them as a warning about the eviction path, not as Hatchet's
capacity. Re-run from a clean database gave a different answer: sizing durable
slots to the fleet is both simpler and much faster than turning eviction on.

## Thousands of sessions, from a clean database

Hatchet given durable slots for every session — which is how you would actually
run it — against Resonate, which has no equivalent setting. 45 second hold.

| engine | sessions | parked | fill | drain | wake p50 / max | failed | worker / server peak |
|---|---:|---:|---:|---:|---:|---:|---|
| Hatchet | 1000 | 1000 | 20.3 s | 64.5 s | 8271 / 14795 ms | 0 | 150 / 408 MB |
| Resonate | 1000 | 1000 | 2.5 s | 15.4 s | 2793 / 10673 ms | 0 | 122 / 144 MB |
| Hatchet | 4000 | 4000 | 98.3 s | did not finish | — | — | 385 / 466 MB |
| Resonate | 4000 | 4000 | 16.3 s | 82.4 s | 20553 / 71588 ms | 0 | 433 / 409 MB |

**Both hold thousands of parked sessions.** 4000 parked with zero failures on
either engine, on four cores.

**Filling is where they separate.** Resonate is 6 to 8 times faster to get
sessions parked — 2.5 s against 20.3 s at 1000, 16.3 s against 98.3 s at 4000.

**Memory per parked session is small.** Resonate's 1000-to-4000 step adds about
104 KB per session in the worker and 88 KB in the server, so roughly 190 KB
all-in: 10,000 sessions is about 2 GB. Hatchet's engine holds more at the same
count — 408 MB against 144 MB at 1000.

**Waking everything at once is the limit on both.** Hatchet took 64.5 s to drain
1000; at 4000 it had not drained after 25 minutes and the run was stopped, so
that cell has no number. Resonate drained 4000 in 82.4 s but with a 72 s tail.

This is the case worth designing around rather than benchmarking further: sessions
normally arrive spread over time and sit parked, which both engines do well. A
whole fleet waking at once — a runner restart, a region reconnecting — is the
expensive event, and staggering it is cheaper than making either engine faster.

Four cores shared between the engine, the worker and Postgres is the smallest
plausible machine for this. Engine-bound figures — Hatchet's fill and drain
especially — are the ones that should improve most on real hardware.

## A signal sent too early is missed

Both engines match a signal only once the workflow has registered its wait. An
earlier version of the capacity run set every delay to 10 ms, so the second signal
was pushed before the workflow reached the second wait, and the run hung with no
error anywhere. If a capacity run stalls, check that the signal delays are longer
than the time the workflow needs to get to the wait.
