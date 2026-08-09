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

## What this does not measure

Holding thousands of parked sessions. Every wait here is satisfied within a
second or two. A session parked on a human review for hours is a different
question: whether the run keeps a worker slot, and what it costs to hold. Hatchet
answers with an eviction policy and `Runs().Restore`; Resonate suspends the task
and the worker keeps nothing. That comparison still needs writing.
