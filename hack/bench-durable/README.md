# Durable-step benchmark: Hatchet vs Resonate

Two harnesses that run the same workload against two durable execution engines,
so their durable-step cost can be compared on one machine.

The workload is a durable function that runs a fixed number of durable steps in
sequence and then returns. A durable step means "run a function, persist its
result, and do not run it again if the workflow replays". Each engine expresses
that with its own primitive:

| | durable step | where the step body runs |
|---|---|---|
| Hatchet | child task, awaited by a durable task | on a worker, after the queue and scheduler |
| Resonate | `ctx.Run`, awaited by the workflow | in the calling process |

The step bodies do no work, so what is timed is the checkpoint path, not the
application logic.

`resonate/` is a separate Go module. The Resonate SDK is not a Hatchet
dependency and this keeps it out of the root `go.mod`.

## Run it

Start Postgres and NATS, then the engine, as in the repository root README, and
export a client token:

```sh
docker compose up -d postgres nats
go run ./cmd/hatchet-migrate
go run ./cmd/hatchet-admin seed
export HATCHET_CLIENT_TOKEN=...    # go run ./cmd/hatchet-admin token create --tenant-id ...
export HATCHET_CLIENT_HOST_PORT=127.0.0.1:7070
export HATCHET_CLIENT_TLS_STRATEGY=none

go run ./hack/bench-durable/hatchet -n 200 -steps 5 -concurrency 16 -out results.jsonl
```

For Resonate, build the server from https://github.com/resonatehq/resonate and
point it at a database:

```sh
RESONATE_STORAGE__TYPE=postgres \
RESONATE_STORAGE__POSTGRES__URL=postgres://hatchet:hatchet@127.0.0.1:5431/resonate \
  resonate serve

cd hack/bench-durable/resonate
go run . -n 200 -steps 5 -concurrency 16 -out ../../../results.jsonl
```

Both print the same fields and append the same JSON line, so the two runs can be
compared directly. Run one engine at a time: both do background work when idle,
which otherwise lands in the other's numbers.

## Crash recovery

`-mode recover` runs one pipeline and writes a line to `-trace` every time a step
body executes: step index, which process ran it, which pipeline it belongs to,
and when. Kill the process part-way and start it again with `-serve-only`, then
read the trace: a step that survived the crash appears once, a step that was
redone appears twice.

Filter the trace by pipeline ID. A restarted worker also picks up pipelines that
earlier runs left pending, and those steps land in the same file — which looks
exactly like steps of the current pipeline going missing.

## Results

One run, 2026-08-09, on a 4 vCPU / 15 GiB Linux box. Both engines used the same
PostgreSQL 15.6 instance, one engine running at a time. Hatchet used the NATS
JetStream queue backend. Resonate server was built from `v0.9.8`; the Go SDK has
no tagged release, so it was pinned at `v0.0.0-20260617195620-152de9b5bc17`.

200 pipelines x 5 durable steps, two repeats per cell, durable steps per second:

| durable step | c=4 | c=16 | c=64 |
|---|---|---|---|
| Hatchet, child task | 32 / 30 | 31 / 40 | 40 / 60 |
| Resonate, `ctx.Run` | 187 / 179 | 328 / 237 | 340 / 300 |
| Resonate, `ctx.RPC` | 17 | 66 | 97 |

Pipeline latency, p50, for the five steps end to end:

| durable step | c=4 | c=16 | c=64 |
|---|---|---|---|
| Hatchet, child task | 608 / 648 ms | 2558 / 1958 ms | 7647 / 5113 ms |
| Resonate, `ctx.Run` | 105 / 108 ms | 219 / 331 ms | 947 / 1037 ms |
| Resonate, `ctx.RPC` | 1156 ms | 1148 ms | 3109 ms |

`ctx.Run` is about 6x faster per step than a Hatchet child task, because it
records the result and runs the body in the same process. It is the one thing
Hatchet has no equivalent for: every Hatchet durable step is a queued task.

`ctx.RPC` is the like-for-like comparison — both hand the step to the server to
dispatch. There Hatchet is ahead at low concurrency and behind at high: 30 vs 17
steps per second at 4 in flight, 60 vs 97 at 64. Same order of magnitude.

Every run executed each step body exactly once (`step_executions_ratio` 1.00).

Crash recovery, three attempts each, killing the worker after two of six steps:

| | steps redone | resumed after |
|---|---|---|
| Hatchet | 1 (the step in flight) | 38, 39, 40 s |
| Resonate | 1 (the step in flight) | 65, 66, 67 s |

Both keep the same promise: completed steps are not redone, the step that was
running when the process died is. The wait is how long each engine takes to
decide the worker is gone.

Read the throughput numbers as shape, not capacity. Four cores is too few for a
Hatchet engine, a worker and Postgres at once, so the Hatchet figures are lower
than a real deployment would give, and its latency at 64 in flight is queueing
against a saturated engine.
