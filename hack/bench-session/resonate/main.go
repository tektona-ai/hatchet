// Command bench-session-resonate measures what a Tektona session start costs on
// top of the work it actually does.
//
// It emulates the same sequence as ../hatchet: start a sandbox, wait for it to
// report running, clone the repositories, wait for the agent runtime to connect,
// start the coding agent, send the prompt. Two of those are waits for something
// outside the workflow, so a sandbox controller runs alongside and settles the
// promise the workflow is parked on.
//
// Every step sleeps for a configured duration instead of doing the real thing,
// so the difference between the measured time and the sum of those durations is
// what the engine costs. The signal carries the time it was sent, so the step
// after the wait can say how long the engine took to wake the workflow.
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

// Resonate names a child promise `<root id>.<n>`, counting every ctx.Run,
// ctx.Sleep and ctx.Promise in the workflow body in order. The controller has to
// settle promises it did not create, so it rebuilds the ID from those positions:
// the two waits are the 2nd and 4th children of the session workflow.
//
// This is the one place the emulation is not symmetric with Hatchet, where the
// waiter is addressed by event key and a filter on the payload instead.
const (
	seqSandboxRunning   = 2
	seqRuntimeConnected = 4
)

type stepInput struct {
	SessionID string `json:"session_id"`
	WorkMS    int    `json:"work_ms"`
	// NotifySeq is the child position of the promise the sandbox controller
	// should settle once this step is done, and NotifyAfterMS how long the real
	// thing would take to get there. Telling the controller from the step body
	// rather than the workflow body matters: a step body runs once, a workflow
	// body runs again on every replay.
	NotifySeq     int `json:"notify_seq"`
	NotifyAfterMS int `json:"notify_after_ms"`
	// SignalSentAtUnixNano is set when this step follows a wait. The step
	// reports how long it was between the controller settling the promise and
	// this body starting, which is the engine's cost to wake a parked workflow.
	//
	// It is measured here, not in the workflow body, because the workflow body
	// runs again on every replay and would recompute the elapsed time against a
	// later clock each time — which silently turns the wake measurement into
	// "time from the signal until the workflow finished".
	SignalSentAtUnixNano int64 `json:"signal_sent_at_unix_nano"`
}

type stepOutput struct {
	SessionID string  `json:"session_id"`
	WakeMS    float64 `json:"wake_ms"`
}

type sessionInput struct {
	SessionID      string `json:"session_id"`
	StartSandboxMS int    `json:"start_sandbox_ms"`
	BootMS         int    `json:"boot_ms"`
	CloneReposMS   int    `json:"clone_repos_ms"`
	ConnectMS      int    `json:"connect_ms"`
	StartAgentMS   int    `json:"start_agent_ms"`
	SendPromptMS   int    `json:"send_prompt_ms"`
}

type sessionOutput struct {
	SessionID       string  `json:"session_id"`
	WakeRunningMS   float64 `json:"wake_running_ms"`
	WakeConnectedMS float64 `json:"wake_connected_ms"`
}

// signal is what the sandbox controller settles the promise with.
// SentAtUnixNano lets the workflow measure the gap between the signal being sent
// and the workflow running again, without the two needing a shared clock.
type signal struct {
	SessionID      string `json:"session_id"`
	SentAtUnixNano int64  `json:"sent_at_unix_nano"`
}

// waitReached is how a workflow tells the sandbox controller that it has got as
// far as a wait, so the controller can start counting down to the signal.
type waitReached struct {
	sessionID string
	promiseID string
	after     time.Duration
}

var reached = make(chan waitReached, 4096)

func work(info resonate.Info, input stepInput) (stepOutput, error) {
	out := stepOutput{SessionID: input.SessionID}

	if input.SignalSentAtUnixNano != 0 {
		out.WakeMS = float64(time.Since(time.Unix(0, input.SignalSentAtUnixNano)).Microseconds()) / 1000.0
	}

	time.Sleep(time.Duration(input.WorkMS) * time.Millisecond)

	if input.NotifySeq > 0 {
		reached <- waitReached{
			sessionID: input.SessionID,
			promiseID: fmt.Sprintf("%s.%d", info.OriginID(), input.NotifySeq),
			after:     time.Duration(input.NotifyAfterMS) * time.Millisecond,
		}
	}

	return out, nil
}

func session(ctx *resonate.Context, input sessionInput) (sessionOutput, error) {
	out := sessionOutput{SessionID: input.SessionID}

	if _, err := runStep(ctx, stepInput{
		SessionID:     input.SessionID,
		WorkMS:        input.StartSandboxMS,
		NotifySeq:     seqSandboxRunning,
		NotifyAfterMS: input.BootMS,
	}); err != nil {
		return out, fmt.Errorf("start sandbox: %w", err)
	}

	sentAt, err := waitForSignal(ctx)
	if err != nil {
		return out, fmt.Errorf("wait for running: %w", err)
	}

	cloned, err := runStep(ctx, stepInput{
		SessionID:            input.SessionID,
		WorkMS:               input.CloneReposMS,
		NotifySeq:            seqRuntimeConnected,
		NotifyAfterMS:        input.ConnectMS,
		SignalSentAtUnixNano: sentAt,
	})
	if err != nil {
		return out, fmt.Errorf("clone repos: %w", err)
	}

	out.WakeRunningMS = cloned.WakeMS

	sentAt, err = waitForSignal(ctx)
	if err != nil {
		return out, fmt.Errorf("wait for runtime connected: %w", err)
	}

	started, err := runStep(ctx, stepInput{
		SessionID:            input.SessionID,
		WorkMS:               input.StartAgentMS,
		SignalSentAtUnixNano: sentAt,
	})
	if err != nil {
		return out, fmt.Errorf("start agent session: %w", err)
	}

	out.WakeConnectedMS = started.WakeMS

	if _, err := runStep(ctx, stepInput{SessionID: input.SessionID, WorkMS: input.SendPromptMS}); err != nil {
		return out, fmt.Errorf("send prompt: %w", err)
	}

	return out, nil
}

func runStep(ctx *resonate.Context, input stepInput) (stepOutput, error) {
	var out stepOutput

	future, err := ctx.Run(work, input)
	if err != nil {
		return out, err
	}

	return out, future.Await(&out)
}

// waitForSignal parks the workflow on a durable promise and returns the time the
// controller settled it, for the next step to measure against.
func waitForSignal(ctx *resonate.Context) (int64, error) {
	future, err := ctx.Promise()
	if err != nil {
		return 0, err
	}

	var sig signal
	if err := future.Await(&sig); err != nil {
		return 0, err
	}

	return sig.SentAtUnixNano, nil
}

func main() {
	var (
		url            = flag.String("url", "http://127.0.0.1:8001", "Resonate server URL")
		n              = flag.Int("n", 50, "session starts in the measured window")
		concurrency    = flag.Int("concurrency", 8, "session starts in flight at once")
		warmup         = flag.Int("warmup", 4, "session starts run and discarded before measuring")
		startSandboxMS = flag.Int("start-sandbox-ms", 50, "work in the start-sandbox call")
		bootMS         = flag.Int("boot-ms", 790, "sandbox boot: start-sandbox until it reports running")
		cloneMS        = flag.Int("clone-ms", 3000, "work in the clone-repos step")
		connectMS      = flag.Int("connect-ms", 1000, "clone finished until the agent runtime connects")
		startAgentMS   = flag.Int("start-agent-ms", 500, "work in the start-agent-session step")
		promptMS       = flag.Int("prompt-ms", 100, "work in the send-prompt step")
		timeoutS       = flag.Int("timeout", 1800, "seconds to wait for the measured window")
		label          = flag.String("label", "resonate", "label for the result line")
		outPath        = flag.String("out", "", "optional path to append a JSON result line to")
	)
	flag.Parse()

	r, err := resonate.New(resonate.Config{URL: *url})
	if err != nil {
		log.Fatalf("failed to create resonate client: %v", err)
	}

	defer func() { _ = r.Stop() }()

	sessionFn, err := resonate.Register(r, "session-start", session)
	if err != nil {
		log.Fatalf("failed to register session workflow: %v", err)
	}

	if _, err := resonate.Register(r, "session-step", work); err != nil {
		log.Fatalf("failed to register step: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutS)*time.Second)
	defer cancel()

	controllerCtx, stopController := context.WithCancel(context.Background())
	defer stopController()

	go runController(controllerCtx, r)

	var seq atomic.Int64

	prefix := fmt.Sprintf("s%d", time.Now().UnixNano())

	var (
		mu    sync.Mutex
		wakes []sessionOutput
	)

	run := func(ctx context.Context) error {
		id := fmt.Sprintf("%s-%d", prefix, seq.Add(1))

		handle, err := sessionFn.Run(ctx, id, sessionInput{
			SessionID:      id,
			StartSandboxMS: *startSandboxMS,
			BootMS:         *bootMS,
			CloneReposMS:   *cloneMS,
			ConnectMS:      *connectMS,
			StartAgentMS:   *startAgentMS,
			SendPromptMS:   *promptMS,
		})
		if err != nil {
			return err
		}

		out, err := handle.Result(ctx)
		if err != nil {
			return err
		}

		mu.Lock()
		wakes = append(wakes, out)
		mu.Unlock()

		return nil
	}

	log.Printf("[%s] warmup: %d session starts", *label, *warmup)

	if _, err := drive(ctx, run, *warmup, min(*concurrency, *warmup)); err != nil {
		log.Fatalf("[%s] warmup failed: %v", *label, err)
	}

	mu.Lock()
	wakes = wakes[:0]
	mu.Unlock()

	log.Printf("[%s] measuring: %d session starts at concurrency %d", *label, *n, *concurrency)

	start := time.Now()

	latencies, err := drive(ctx, run, *n, *concurrency)
	if err != nil {
		log.Fatalf("[%s] run failed: %v", *label, err)
	}

	total := time.Since(start)

	workMS := *startSandboxMS + *bootMS + *cloneMS + *connectMS + *startAgentMS + *promptMS

	mu.Lock()
	defer mu.Unlock()

	report(*label, "resonate", *n, *concurrency, workMS, total, latencies, wakes, *outPath)
}

// runController stands in for the sandbox controller and the agent gateway: it
// waits the time the real thing would take, then settles the promise the
// workflow is parked on.
//
// Settling retries, because the step that told us about the wait finishes just
// before the workflow replays and creates the promise — for a moment there is
// nothing to settle yet.
func runController(ctx context.Context, r *resonate.Resonate) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-reached:
			go func(req waitReached) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(req.after):
				}

				for attempt := range 100 {
					_, err := r.Promises().Resolve(ctx, req.promiseID, signal{
						SessionID:      req.sessionID,
						SentAtUnixNano: time.Now().UnixNano(),
					})
					if err == nil {
						return
					}

					if ctx.Err() != nil {
						return
					}

					if attempt == 99 {
						log.Printf("gave up settling %s: %v", req.promiseID, err)
						return
					}

					time.Sleep(20 * time.Millisecond)
				}
			}(req)
		}
	}
}

// drive runs count session starts with at most concurrency in flight and
// returns one latency per session start.
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

func report(label, engine string, n, concurrency, workMS int, total time.Duration, latencies []time.Duration, wakes []sessionOutput, outPath string) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	running := make([]float64, 0, len(wakes))
	connected := make([]float64, 0, len(wakes))

	for _, w := range wakes {
		running = append(running, w.WakeRunningMS)
		connected = append(connected, w.WakeConnectedMS)
	}

	sort.Float64s(running)
	sort.Float64s(connected)

	p50 := ms(pct(latencies, 0.50))

	res := map[string]any{
		"label":                 label,
		"engine":                engine,
		"sessions":              n,
		"concurrency":           concurrency,
		"emulated_work_ms":      workMS,
		"total_seconds":         total.Seconds(),
		"sessions_per_s":        float64(n) / total.Seconds(),
		"session_p50_ms":        p50,
		"session_p95_ms":        ms(pct(latencies, 0.95)),
		"session_p99_ms":        ms(pct(latencies, 0.99)),
		"session_max_ms":        ms(latencies[len(latencies)-1]),
		"engine_overhead_ms":    p50 - float64(workMS),
		"wake_running_p50_ms":   pctF(running, 0.50),
		"wake_running_p95_ms":   pctF(running, 0.95),
		"wake_connected_p50_ms": pctF(connected, 0.50),
		"wake_connected_p95_ms": pctF(connected, 0.95),
	}

	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("sessions=%d concurrency=%d emulated work=%dms\n", n, concurrency, workMS)
	fmt.Printf("total        %.2fs  (%.2f session starts/s)\n", total.Seconds(), res["sessions_per_s"])
	fmt.Printf("session      p50=%.0fms p95=%.0fms p99=%.0fms max=%.0fms\n",
		res["session_p50_ms"], res["session_p95_ms"], res["session_p99_ms"], res["session_max_ms"])
	fmt.Printf("engine cost  %.0fms on top of %dms of work\n", res["engine_overhead_ms"], workMS)
	fmt.Printf("wake         running p50=%.0fms p95=%.0fms   connected p50=%.0fms p95=%.0fms\n",
		res["wake_running_p50_ms"], res["wake_running_p95_ms"],
		res["wake_connected_p50_ms"], res["wake_connected_p95_ms"])

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

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	return sorted[int(float64(len(sorted)-1)*p)]
}

func pctF(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	return sorted[int(float64(len(sorted)-1)*p)]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
