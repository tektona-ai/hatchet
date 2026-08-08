// Command bench measures end-to-end task throughput and latency against a
// running Hatchet engine, so the durable message queue backends can be compared
// on identical hardware.
//
// It registers one trivial task, bulk-enqueues N of them, and waits for the
// worker to execute every one. The task body does no work: what is being timed
// is the enqueue -> queue -> scheduler -> dispatcher -> worker path, which is
// where the message queue backend lives.
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

type benchInput struct {
	// EnqueuedAtUnixNano is stamped by the producer so the handler can measure
	// the full enqueue-to-execution latency without a shared clock.
	EnqueuedAtUnixNano int64 `json:"enqueued_at_unix_nano"`
	Seq                int   `json:"seq"`
}

type benchOutput struct {
	OK bool `json:"ok"`
}

func main() {
	var (
		n         = flag.Int("n", 500, "number of tasks to enqueue")
		slots     = flag.Int("slots", 100, "worker slots (max concurrent task executions)")
		batch     = flag.Int("batch", 100, "tasks per RunMany call")
		label     = flag.String("label", "unknown", "backend label for the report line")
		warmup    = flag.Int("warmup", 20, "warmup tasks executed and excluded from the report")
		timeoutS  = flag.Int("timeout", 300, "seconds to wait for all tasks to complete")
		outPath   = flag.String("out", "", "optional path to append a JSON result line to")
		taskName  = flag.String("task", "bench-task", "task name to register")
		producers = flag.Int("producers", 16, "concurrent enqueue goroutines")
	)
	flag.Parse()

	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("failed to create hatchet client: %v", err)
	}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		done      atomic.Int64
	)

	// completed is closed once the expected number of tasks have executed.
	expected := int64(*warmup + *n)
	completed := make(chan struct{})
	var closeOnce sync.Once

	task := client.NewStandaloneTask(*taskName, func(ctx hatchet.Context, input benchInput) (benchOutput, error) {
		if input.Seq >= 0 {
			// Negative sequence numbers are warmup tasks: they pay the
			// first-run costs (worker registration, connection setup, query
			// plan caching) that would otherwise land in the measurement.
			lat := time.Since(time.Unix(0, input.EnqueuedAtUnixNano))

			mu.Lock()
			latencies = append(latencies, lat)
			mu.Unlock()
		}

		if done.Add(1) >= expected {
			closeOnce.Do(func() { close(completed) })
		}

		return benchOutput{OK: true}, nil
	})

	worker, err := client.NewWorker("bench-worker",
		hatchet.WithWorkflows(task),
		hatchet.WithSlots(*slots),
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

	// Give the worker time to register and its dispatcher stream to be ready.
	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutS)*time.Second)
	defer cancel()

	log.Printf("[%s] enqueueing %d warmup tasks", *label, *warmup)
	enqueue(ctx, task, *warmup, *batch, *producers, true)

	// Wait for warmup to drain so its execution does not overlap the measured
	// window; otherwise the first measured tasks would queue behind it.
	waitFor(func() bool { return done.Load() >= int64(*warmup) }, 60*time.Second)

	log.Printf("[%s] enqueueing %d tasks", *label, *n)

	start := time.Now()
	enqueueElapsed := enqueue(ctx, task, *n, *batch, *producers, false)

	select {
	case <-completed:
	case <-ctx.Done():
		log.Fatalf("[%s] timed out: %d/%d tasks completed", *label, done.Load(), expected)
	}

	total := time.Since(start)

	mu.Lock()
	defer mu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	res := map[string]any{
		"backend":          *label,
		"tasks":            *n,
		"slots":            *slots,
		"producers":        *producers,
		"total_seconds":    total.Seconds(),
		"throughput_per_s": float64(*n) / total.Seconds(),
		"enqueue_seconds":  enqueueElapsed.Seconds(),
		"enqueue_per_s":    float64(*n) / enqueueElapsed.Seconds(),
		"latency_p50_ms":   ms(pct(latencies, 0.50)),
		"latency_p95_ms":   ms(pct(latencies, 0.95)),
		"latency_p99_ms":   ms(pct(latencies, 0.99)),
		"latency_max_ms":   ms(latencies[len(latencies)-1]),
	}

	fmt.Printf("\n=== %s ===\n", *label)
	fmt.Printf("tasks=%d slots=%d\n", *n, *slots)
	fmt.Printf("total       %.2fs  (%.1f tasks/s end-to-end)\n", total.Seconds(), float64(*n)/total.Seconds())
	fmt.Printf("enqueue     %.2fs  (%.1f tasks/s)\n", enqueueElapsed.Seconds(), float64(*n)/enqueueElapsed.Seconds())
	fmt.Printf("latency     p50=%.0fms p95=%.0fms p99=%.0fms max=%.0fms\n",
		res["latency_p50_ms"], res["latency_p95_ms"], res["latency_p99_ms"], res["latency_max_ms"])

	if *outPath != "" {
		f, err := os.OpenFile(*outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			enc := json.NewEncoder(f)
			_ = enc.Encode(res)
			_ = f.Close()
		}
	}

	stopWorker()
	time.Sleep(500 * time.Millisecond)
}

// enqueue submits count tasks in batches and returns how long submission took.
//
// Submission runs across `producers` goroutines because a single producer is
// slower than the engine drains: with one, total time equals submission time
// and the benchmark measures the gRPC ingest path instead of the message queue.
// Enough producers to make the queue back up is what puts the backend under
// test.
func enqueue(ctx context.Context, task *hatchet.StandaloneTask, count, batch, producers int, isWarmup bool) time.Duration {
	start := time.Now()

	type job struct{ offset, size int }

	jobs := make(chan job)

	var wg sync.WaitGroup

	for range producers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for jb := range jobs {
				opts := make([]hatchet.RunManyOpt, jb.size)

				for j := range opts {
					seq := jb.offset + j
					if isWarmup {
						seq = -1
					}

					opts[j] = hatchet.RunManyOpt{
						Input: benchInput{
							EnqueuedAtUnixNano: time.Now().UnixNano(),
							Seq:                seq,
						},
					}
				}

				if _, err := task.RunMany(ctx, opts); err != nil {
					log.Printf("failed to enqueue batch at %d: %v", jb.offset, err)
				}
			}
		}()
	}

	for i := 0; i < count; i += batch {
		size := batch
		if i+size > count {
			size = count - i
		}

		jobs <- job{offset: i, size: size}
	}

	close(jobs)
	wg.Wait()

	return time.Since(start)
}

func waitFor(cond func() bool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	idx := int(float64(len(sorted)-1) * p)

	return sorted[idx]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
