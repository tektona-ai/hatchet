// Command bench-durable-hatchet measures durable-step throughput and latency
// against a running Hatchet engine.
//
// The workload is a durable task that runs a fixed number of durable steps in
// sequence and then returns. A step is a child task: that is Hatchet's unit of
// "run a function, record its result", so a completed step is not re-executed
// when the parent replays. Step bodies do no work, so what is timed is the
// checkpoint path — durable event log, queue, scheduler, dispatcher, worker.
//
// The same workload runs against Resonate in ../resonate, with the same flags
// and the same result line, so the two can be compared on one machine.
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

	hatchet "github.com/hatchet-dev/hatchet/sdks/go"
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

func main() {
	var (
		n           = flag.Int("n", 200, "number of pipelines to run in the measured window")
		steps       = flag.Int("steps", 5, "durable steps per pipeline")
		concurrency = flag.Int("concurrency", 16, "pipelines in flight at once")
		warmup      = flag.Int("warmup", 8, "pipelines run and discarded before measuring")
		slots       = flag.Int("slots", 200, "worker slots for step tasks")
		timeoutS    = flag.Int("timeout", 900, "seconds to wait for the measured window")
		label       = flag.String("label", "hatchet-nats", "label for the result line")
		outPath     = flag.String("out", "", "optional path to append a JSON result line to")

		mode      = flag.String("mode", "bench", "bench, or recover for the crash-recovery run")
		traceTag  = flag.String("trace-tag", "a", "recover mode: label for this process in the trace file")
		trace     = flag.String("trace", "", "recover mode: file each step body appends to when it executes")
		stepDelay = flag.Duration("step-delay", 0, "recover mode: how long each step body takes")
		serveOnly = flag.Bool("serve-only", false, "recover mode: serve without triggering, for the restarted process")
	)
	flag.Parse()

	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("failed to create hatchet client: %v", err)
	}

	// stepExecutions counts how many times a step body actually ran. Against
	// the number of steps the pipelines asked for, it shows whether any
	// completed step was re-executed.
	var stepExecutions atomic.Int64

	step := client.NewStandaloneTask("bench-durable-step", func(ctx hatchet.Context, input stepInput) (stepOutput, error) {
		stepExecutions.Add(1)
		appendTrace(*trace, *traceTag, pipelineRunID(ctx), input.Idx)

		if *stepDelay > 0 {
			time.Sleep(*stepDelay)
		}

		return stepOutput{Idx: input.Idx}, nil
	})

	pipeline := client.NewStandaloneDurableTask("bench-durable-pipeline", func(ctx hatchet.DurableContext, input pipelineInput) (pipelineOutput, error) {
		for i := range input.Steps {
			if _, err := step.Run(ctx, stepInput{Idx: i}); err != nil {
				return pipelineOutput{}, fmt.Errorf("step %d: %w", i, err)
			}
		}

		return pipelineOutput{Steps: input.Steps}, nil
	})

	worker, err := client.NewWorker("bench-durable-worker",
		hatchet.WithWorkflows(step, pipeline),
		hatchet.WithSlots(*slots),
		hatchet.WithDurableSlots(*concurrency*2),
	)
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}

	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	go func() {
		if err := worker.StartBlocking(workerCtx); err != nil && workerCtx.Err() == nil {
			log.Fatalf("worker failed: %v", err)
		}
	}()

	// Let the worker register and its dispatcher streams settle before the
	// first run, so connection setup does not land in the measurement.
	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutS)*time.Second)
	defer cancel()

	if *mode == "recover" {
		// The first process triggers one pipeline and then serves until it is
		// killed. The restarted process only serves, so the engine hands it the
		// same pipeline to finish.
		if !*serveOnly {
			ref, err := pipeline.RunNoWait(ctx, pipelineInput{Steps: *steps})
			if err != nil {
				log.Fatalf("failed to trigger pipeline: %v", err)
			}

			log.Printf("triggered pipeline %s", ref.RunId)
		}

		<-ctx.Done()

		return
	}

	run := func(ctx context.Context) error {
		_, err := pipeline.Run(ctx, pipelineInput{Steps: *steps})
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

	report(*label, "hatchet", *n, *steps, *concurrency, total, latencies, stepExecutions.Load(), *outPath)

	stopWorker()
	time.Sleep(500 * time.Millisecond)
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

// pipelineRunID is the run this step belongs to: the parent durable task for a
// child step, or the step's own run when there is no parent.
func pipelineRunID(ctx hatchet.Context) string {
	if parent := ctx.ParentWorkflowRunId(); parent != nil {
		return *parent
	}

	return ctx.WorkflowRunId()
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
