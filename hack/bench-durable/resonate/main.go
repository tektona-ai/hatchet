// Command bench-durable-resonate measures durable-step throughput and latency
// against a running Resonate server.
//
// The workload matches ../hatchet: a durable function that runs a fixed number
// of durable steps in sequence and then returns. A step is a ctx.Run call —
// Resonate's unit of "run a function, persist its result" — so a completed step
// is not re-executed when the workflow replays. Step bodies do no work, so what
// is timed is the checkpoint path: the durable promise writes on the server.
//
// This is a separate Go module on purpose: the Resonate SDK is not a dependency
// of Hatchet, and keeping it out of the root go.mod keeps the fork's module
// graph unchanged.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

type stepInput struct {
	Idx int `json:"idx"`
}

type stepOutput struct {
	Idx int `json:"idx"`
}

type pipelineInput struct {
	Steps int `json:"steps"`
}

type pipelineOutput struct {
	Steps int `json:"steps"`
}

// stepExecutions counts how many times a step body actually ran. Against the
// number of steps the pipelines asked for, it shows whether any completed step
// was re-executed. It is a package-level counter because Resonate passes the
// step function by value, not as a closure.
var stepExecutions atomic.Int64

// tracePath and stepDelay are set once from flags before any step runs. They are
// package-level for the same reason stepExecutions is.
var (
	tracePath string
	traceTag  string
	stepDelay time.Duration
)

func step(info resonate.Info, input stepInput) (stepOutput, error) {
	stepExecutions.Add(1)
	appendTrace(tracePath, traceTag, info.OriginID(), input.Idx)

	if stepDelay > 0 {
		time.Sleep(stepDelay)
	}

	return stepOutput{Idx: input.Idx}, nil
}

// stepMode selects which durable primitive a step uses. "local" is ctx.Run,
// which executes the body in the calling process. "rpc" is ctx.RPC, which hands
// the step back to the server to dispatch to a worker — the same trip Hatchet's
// child tasks make, so it is the closer comparison.
var stepMode string

func pipeline(ctx *resonate.Context, input pipelineInput) (pipelineOutput, error) {
	for i := range input.Steps {
		var (
			future *resonate.Future
			err    error
		)

		if stepMode == "rpc" {
			future, err = ctx.RPC("bench-durable-step", stepInput{Idx: i})
		} else {
			future, err = ctx.Run(step, stepInput{Idx: i})
		}

		if err != nil {
			return pipelineOutput{}, fmt.Errorf("step %d: %w", i, err)
		}

		var out stepOutput
		if err := future.Await(&out); err != nil {
			return pipelineOutput{}, fmt.Errorf("await step %d: %w", i, err)
		}
	}

	return pipelineOutput{Steps: input.Steps}, nil
}

func main() {
	var (
		url         = flag.String("url", "http://127.0.0.1:8001", "Resonate server URL")
		n           = flag.Int("n", 200, "number of pipelines to run in the measured window")
		steps       = flag.Int("steps", 5, "durable steps per pipeline")
		concurrency = flag.Int("concurrency", 16, "pipelines in flight at once")
		warmup      = flag.Int("warmup", 8, "pipelines run and discarded before measuring")
		timeoutS    = flag.Int("timeout", 900, "seconds to wait for the measured window")
		label       = flag.String("label", "resonate", "label for the result line")
		outPath     = flag.String("out", "", "optional path to append a JSON result line to")

		steppy = flag.String("step-mode", "local", "local for ctx.Run, rpc for ctx.RPC")

		mode      = flag.String("mode", "bench", "bench, or recover for the crash-recovery run")
		runID     = flag.String("run-id", "", "recover mode: promise ID, shared by both processes")
		trace     = flag.String("trace", "", "recover mode: file each step body appends to when it executes")
		tag       = flag.String("trace-tag", "a", "recover mode: label for this process in the trace file")
		delay     = flag.Duration("step-delay", 0, "recover mode: how long each step body takes")
		serveOnly = flag.Bool("serve-only", false, "recover mode: serve without starting a pipeline, for the restarted process")
	)
	flag.Parse()

	tracePath = *trace
	traceTag = *tag
	stepDelay = *delay
	stepMode = *steppy

	r, err := resonate.New(resonate.Config{URL: *url})
	if err != nil {
		log.Fatalf("failed to create resonate client: %v", err)
	}

	defer func() { _ = r.Stop() }()

	pipelineFn, err := resonate.Register(r, "bench-durable-pipeline", pipeline)
	if err != nil {
		log.Fatalf("failed to register pipeline: %v", err)
	}

	if _, err := resonate.Register(r, "bench-durable-step", step); err != nil {
		log.Fatalf("failed to register step: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutS)*time.Second)
	defer cancel()

	if *mode == "recover" {
		// The first process starts one pipeline and then serves until it is
		// killed. The restarted process only serves, so the server hands it the
		// same pipeline to finish once the first process's lease expires.
		// Restarting with Run instead would let both the restart and the lease
		// expiry dispatch the workflow, and the two replays would race.
		if *serveOnly {
			log.Printf("serving, not starting a pipeline")
			<-ctx.Done()

			return
		}

		handle, err := pipelineFn.Run(ctx, *runID, pipelineInput{Steps: *steps})
		if err != nil {
			log.Fatalf("failed to start pipeline: %v", err)
		}

		log.Printf("pipeline %s running", handle.ID())

		if _, err := handle.Result(ctx); err != nil {
			log.Fatalf("pipeline failed: %v", err)
		}

		log.Printf("pipeline %s completed", handle.ID())

		return
	}

	// Each invocation needs a unique ID: Resonate treats a repeated ID as a
	// handle to the existing promise, which would return the first result
	// instead of doing the work again.
	var seq atomic.Int64

	runPrefix := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	run := func(ctx context.Context) error {
		id := fmt.Sprintf("%s-%d", runPrefix, seq.Add(1))

		handle, err := pipelineFn.Run(ctx, id, pipelineInput{Steps: *steps})
		if err != nil {
			return err
		}

		_, err = handle.Result(ctx)

		return err
	}

	log.Printf("[%s] warmup: %d pipelines", *label, *warmup)

	if _, err := drive(ctx, run, *warmup, min(*concurrency, *warmup)); err != nil {
		log.Fatalf("[%s] warmup failed: %v", *label, err)
	}

	stepExecutions.Store(0)

	log.Printf("[%s] measuring: %d pipelines x %d steps at concurrency %d", *label, *n, *steps, *concurrency)

	start := time.Now()

	latencies, err := drive(ctx, run, *n, *concurrency)
	if err != nil {
		log.Fatalf("[%s] run failed: %v", *label, err)
	}

	total := time.Since(start)

	report(*label, "resonate", *n, *steps, *concurrency, total, latencies, stepExecutions.Load(), *outPath)
}

// drive runs count pipelines with at most concurrency in flight and returns
// one latency per pipeline.
func drive(ctx context.Context, run func(context.Context) error, count, concurrency int) ([]time.Duration, error) {
	if concurrency < 1 {
		concurrency = 1
	}

	var (
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, count)
		remaining atomic.Int64
		firstErr  error
		errOnce   sync.Once
	)

	remaining.Store(int64(count))

	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for remaining.Add(-1) >= 0 {
				started := time.Now()

				if err := run(ctx); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}

				lat := time.Since(started)

				mu.Lock()
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return latencies, nil
}

func report(label, engine string, n, steps, concurrency int, total time.Duration, latencies []time.Duration, stepExecutions int64, outPath string) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	durableSteps := n * steps

	res := map[string]any{
		"label":                 label,
		"engine":                engine,
		"pipelines":             n,
		"steps_per_pipeline":    steps,
		"concurrency":           concurrency,
		"total_seconds":         total.Seconds(),
		"pipelines_per_s":       float64(n) / total.Seconds(),
		"durable_steps_per_s":   float64(durableSteps) / total.Seconds(),
		"step_executions":       stepExecutions,
		"step_executions_ratio": float64(stepExecutions) / float64(durableSteps),
		"pipeline_p50_ms":       ms(pct(latencies, 0.50)),
		"pipeline_p95_ms":       ms(pct(latencies, 0.95)),
		"pipeline_p99_ms":       ms(pct(latencies, 0.99)),
		"pipeline_max_ms":       ms(latencies[len(latencies)-1]),
	}

	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("pipelines=%d steps=%d concurrency=%d\n", n, steps, concurrency)
	fmt.Printf("total       %.2fs\n", total.Seconds())
	fmt.Printf("throughput  %.1f pipelines/s  %.1f durable steps/s\n", res["pipelines_per_s"], res["durable_steps_per_s"])
	fmt.Printf("pipeline    p50=%.0fms p95=%.0fms p99=%.0fms max=%.0fms\n",
		res["pipeline_p50_ms"], res["pipeline_p95_ms"], res["pipeline_p99_ms"], res["pipeline_max_ms"])
	fmt.Printf("step bodies %d executed for %d requested (%.2fx)\n", stepExecutions, durableSteps, res["step_executions_ratio"])

	if outPath == "" {
		return
	}

	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("could not write result line: %v", err)
		return
	}

	defer func() { _ = f.Close() }()

	if err := json.NewEncoder(f).Encode(res); err != nil {
		log.Printf("could not encode result line: %v", err)
	}
}

// appendTrace records one line per step body execution — step index, which
// process ran it, which pipeline it belongs to, and when — so a crash run can be
// checked for steps that ran twice. The pipeline ID matters: a worker also picks
// up pipelines abandoned by earlier runs, and those steps land in the same file.
func appendTrace(path, tag, pipelineID string, idx int) {
	if path == "" {
		return
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("could not write trace line: %v", err)
		return
	}

	defer func() { _ = f.Close() }()

	if _, err := fmt.Fprintf(f, "%d %s %s %s\n", idx, tag, pipelineID, time.Now().Format(time.RFC3339Nano)); err != nil {
		log.Printf("could not write trace line: %v", err)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	return sorted[int(float64(len(sorted)-1)*p)]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
