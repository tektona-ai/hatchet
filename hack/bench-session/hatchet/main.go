// Command bench-session-hatchet measures what a Tektona session start costs on
// top of the work it actually does.
//
// It emulates the sequence a thread runs when a session begins: start a sandbox,
// wait for it to report running, clone the repositories, wait for the agent
// runtime to connect, start the coding agent, send the prompt. Two of those are
// waits for something outside the workflow, so a sandbox controller runs
// alongside and signals when a wait should be satisfied.
//
// Every step sleeps for a configured duration instead of doing the real thing,
// so the difference between the measured time and the sum of those durations is
// what the engine costs. The two waits are timed separately: the signal carries
// the time it was sent, so the step after the wait can say how long the engine
// took to wake the workflow.
//
// The same emulation runs against Resonate in ../resonate.
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

const (
	eventSandboxRunning   = "sandbox:running"
	eventRuntimeConnected = "runtime:connected"

	// How often the controller repeats an announcement, and how many times.
	signalRepeats     = 6
	signalRepeatEvery = 5 * time.Second
)

type stepInput struct {
	SessionID string `json:"session_id"`
	WorkMS    int    `json:"work_ms"`
	// Notify names the event the sandbox controller should send once this step
	// is done, and NotifyAfterMS how long the real thing would take to get
	// there. Telling the controller from the step body rather than the workflow
	// body matters: a step body runs once, a workflow body runs again on every
	// replay.
	Notify        string `json:"notify"`
	NotifyAfterMS int    `json:"notify_after_ms"`
	// SignalSentAtUnixNano is set when this step follows a wait. The step
	// reports how long it was between the controller sending the signal and
	// this body starting, which is the engine's cost to wake a parked workflow.
	//
	// It is measured here, not in the workflow body, because a workflow body
	// can run again on replay and would recompute the elapsed time against a
	// later clock every time.
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

// signal is what the sandbox controller pushes. SentAtUnixNano lets the
// workflow measure the gap between the signal being sent and the workflow
// running again, without the two needing a shared clock.
type signal struct {
	SessionID      string `json:"session_id"`
	SentAtUnixNano int64  `json:"sent_at_unix_nano"`
}

// waitReached is how a workflow tells the sandbox controller that it has got as
// far as a wait, so the controller can start counting down to the signal.
type waitReached struct {
	sessionID string
	eventKey  string
	after     time.Duration
}

var (
	reached = make(chan waitReached, 65536)

	// Capacity mode holds every session at the first wait instead of signalling
	// it after a delay, so the run can be filled to N parked sessions and then
	// released all at once.
	capacityMode bool
	parked       atomic.Int64
	release      = make(chan struct{})
)

func main() {
	var (
		n              = flag.Int("n", 50, "session starts in the measured window")
		concurrency    = flag.Int("concurrency", 8, "session starts in flight at once")
		warmup         = flag.Int("warmup", 4, "session starts run and discarded before measuring")
		startSandboxMS = flag.Int("start-sandbox-ms", 50, "work in the start-sandbox call")
		bootMS         = flag.Int("boot-ms", 790, "sandbox boot: start-sandbox until it reports running")
		cloneMS        = flag.Int("clone-ms", 3000, "work in the clone-repos step")
		connectMS      = flag.Int("connect-ms", 1000, "clone finished until the agent runtime connects")
		startAgentMS   = flag.Int("start-agent-ms", 500, "work in the start-agent-session step")
		promptMS       = flag.Int("prompt-ms", 100, "work in the send-prompt step")
		slots          = flag.Int("slots", 400, "worker slots for step tasks")
		durableSlots   = flag.Int("durable-slots", 0, "durable slots; 0 means twice the concurrency")
		timeoutS       = flag.Int("timeout", 1800, "seconds to wait for the measured window")
		label          = flag.String("label", "hatchet-nats", "label for the result line")
		outPath        = flag.String("out", "", "optional path to append a JSON result line to")

		evictionTTL      = flag.Duration("eviction-ttl", 0, "capacity mode: evict a parked session off the worker after this long; 0 disables eviction")
		executionTimeout = flag.Duration("execution-timeout", 10*time.Minute, "how long a session workflow may take before the engine gives up on it")

		mode     = flag.String("mode", "session", "session for the latency run, capacity for the parked-session run")
		holdS    = flag.Int("hold-seconds", 30, "capacity mode: how long to hold every session parked before releasing")
		fillWait = flag.Int("fill-timeout", 600, "capacity mode: seconds to wait for every session to park")
	)
	flag.Parse()

	capacityMode = *mode == "capacity"

	if *durableSlots == 0 {
		*durableSlots = *concurrency * 2
	}

	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("failed to create hatchet client: %v", err)
	}

	work := func(ctx hatchet.Context, input stepInput) (stepOutput, error) {
		out := stepOutput{SessionID: input.SessionID}

		if input.SignalSentAtUnixNano != 0 {
			out.WakeMS = float64(time.Since(time.Unix(0, input.SignalSentAtUnixNano)).Microseconds()) / 1000.0
		}

		time.Sleep(time.Duration(input.WorkMS) * time.Millisecond)

		if input.Notify != "" {
			reached <- waitReached{
				sessionID: input.SessionID,
				eventKey:  input.Notify,
				after:     time.Duration(input.NotifyAfterMS) * time.Millisecond,
			}
		}

		return out, nil
	}

	startSandbox := client.NewStandaloneTask("session-start-sandbox", work)
	cloneRepos := client.NewStandaloneTask("session-clone-repos", work)
	startAgent := client.NewStandaloneTask("session-start-agent", work)
	sendPrompt := client.NewStandaloneTask("session-send-prompt", work)

	sessionOptions := []hatchet.StandaloneTaskOption{hatchet.WithExecutionTimeout(*executionTimeout)}

	// Without an eviction policy a parked session keeps its durable slot on the
	// worker, so the number of sessions that can wait at once is however many
	// durable slots the fleet has. An evictable session gives the slot back
	// while it waits and is restored when its event arrives.
	if *evictionTTL > 0 {
		sessionOptions = append(sessionOptions, hatchet.WithEvictionPolicy(&hatchet.EvictionPolicy{
			TTL:                   *evictionTTL,
			AllowCapacityEviction: true,
		}))
	}

	session := client.NewStandaloneDurableTask("session-start", func(ctx hatchet.DurableContext, input sessionInput) (sessionOutput, error) {
		out := sessionOutput{SessionID: input.SessionID}

		if _, err := runStep(ctx, startSandbox, stepInput{
			SessionID:     input.SessionID,
			WorkMS:        input.StartSandboxMS,
			Notify:        eventSandboxRunning,
			NotifyAfterMS: input.BootMS,
		}); err != nil {
			return out, fmt.Errorf("start sandbox: %w", err)
		}

		sentAt, err := waitForSignal(ctx, eventSandboxRunning, input.SessionID)
		if err != nil {
			return out, fmt.Errorf("wait for running: %w", err)
		}

		cloned, err := runStep(ctx, cloneRepos, stepInput{
			SessionID:            input.SessionID,
			WorkMS:               input.CloneReposMS,
			Notify:               eventRuntimeConnected,
			NotifyAfterMS:        input.ConnectMS,
			SignalSentAtUnixNano: sentAt,
		})
		if err != nil {
			return out, fmt.Errorf("clone repos: %w", err)
		}

		out.WakeRunningMS = cloned.WakeMS

		sentAt, err = waitForSignal(ctx, eventRuntimeConnected, input.SessionID)
		if err != nil {
			return out, fmt.Errorf("wait for runtime connected: %w", err)
		}

		started, err := runStep(ctx, startAgent, stepInput{
			SessionID:            input.SessionID,
			WorkMS:               input.StartAgentMS,
			SignalSentAtUnixNano: sentAt,
		})
		if err != nil {
			return out, fmt.Errorf("start agent session: %w", err)
		}

		out.WakeConnectedMS = started.WakeMS

		if _, err := runStep(ctx, sendPrompt, stepInput{SessionID: input.SessionID, WorkMS: input.SendPromptMS}); err != nil {
			return out, fmt.Errorf("send prompt: %w", err)
		}

		return out, nil
	}, sessionOptions...)

	worker, err := client.NewWorker("bench-session-worker",
		hatchet.WithWorkflows(startSandbox, cloneRepos, startAgent, sendPrompt, session),
		hatchet.WithSlots(*slots),
		hatchet.WithDurableSlots(*durableSlots),
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

	go runController(workerCtx, client)

	// Let the worker register and its dispatcher streams settle before the
	// first run, so connection setup does not land in the measurement.
	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutS)*time.Second)
	defer cancel()

	var seq atomic.Int64

	prefix := fmt.Sprintf("s%d", time.Now().UnixNano())

	var (
		mu    sync.Mutex
		wakes []sessionOutput
	)

	run := func(ctx context.Context) error {
		result, err := session.Run(ctx, sessionInput{
			SessionID:      fmt.Sprintf("%s-%d", prefix, seq.Add(1)),
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

		var out sessionOutput
		if err := result.Into(&out); err != nil {
			return err
		}

		mu.Lock()
		wakes = append(wakes, out)
		mu.Unlock()

		return nil
	}

	if capacityMode {
		runCapacity(ctx, *label, run, *n, *holdS, *fillWait, &mu, &wakes, *outPath)
		stopWorker()
		time.Sleep(500 * time.Millisecond)

		return
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

	report(*label, "hatchet", *n, *concurrency, workMS, total, latencies, wakes, *outPath)

	stopWorker()
	time.Sleep(500 * time.Millisecond)
}

// waitForSignal parks the workflow on an event and returns the time the
// controller sent it, for the next step to measure against.
func waitForSignal(ctx hatchet.DurableContext, key, sessionID string) (int64, error) {
	event, err := ctx.WaitForEvent(key, fmt.Sprintf("input.session_id == '%s'", sessionID))
	if err != nil {
		return 0, err
	}

	var sig signal
	if err := hatchet.EventInto(event, &sig); err != nil {
		return 0, err
	}

	return sig.SentAtUnixNano, nil
}

// runStep runs one child task and decodes its result.
func runStep(ctx hatchet.DurableContext, task *hatchet.StandaloneTask, input stepInput) (stepOutput, error) {
	var out stepOutput

	result, err := task.Run(ctx, input)
	if err != nil {
		return out, err
	}

	return out, result.Into(&out)
}

// runController stands in for the sandbox controller and the agent gateway: it
// waits the time the real thing would take, then pushes the event the workflow
// is parked on.
func runController(ctx context.Context, client *hatchet.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-reached:
			go func(req waitReached) {
				if capacityMode && req.eventKey == eventSandboxRunning {
					// Park here and stay parked until the run is released.
					parked.Add(1)

					select {
					case <-ctx.Done():
						return
					case <-release:
					}
				} else {
					select {
					case <-ctx.Done():
						return
					case <-time.After(req.after):
					}
				}

				// An event only reaches a workflow that has already registered
				// its wait; one pushed a moment too early is dropped and the
				// session waits forever. The workflow can be slow to get there
				// under load, so the controller repeats the announcement the way
				// a reconciler reporting current state would. Repeats after the
				// wait is satisfied match nothing.
				for attempt := range signalRepeats {
					if attempt > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(signalRepeatEvery):
						}
					}

					err := client.Events().Push(ctx, req.eventKey, signal{
						SessionID:      req.sessionID,
						SentAtUnixNano: time.Now().UnixNano(),
					})
					if err != nil && ctx.Err() == nil {
						log.Printf("failed to push %s for %s: %v", req.eventKey, req.sessionID, err)
					}
				}
			}(req)
		}
	}
}

// runCapacity starts n session starts at once, waits for every one of them to
// park on the first wait, holds them there, then releases them together. What it
// measures is how many sessions an engine will hold parked and what it costs to
// wake all of them.
func runCapacity(ctx context.Context, label string, run func(context.Context) error, n, holdS, fillWait int, mu *sync.Mutex, wakes *[]sessionOutput, outPath string) {
	var (
		wg       sync.WaitGroup
		failed   atomic.Int64
		firstErr atomic.Value
	)

	log.Printf("[%s] filling: starting %d sessions", label, n)

	fillStart := time.Now()

	for range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := run(ctx); err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
			}
		}()
	}

	deadline := time.Now().Add(time.Duration(fillWait) * time.Second)
	for parked.Load() < int64(n) && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(250 * time.Millisecond)
	}

	fill := time.Since(fillStart)
	peak := parked.Load()

	log.Printf("[%s] parked %d/%d after %.1fs", label, peak, n, fill.Seconds())

	log.Printf("[%s] holding for %ds", label, holdS)
	time.Sleep(time.Duration(holdS) * time.Second)

	held := parked.Load()

	log.Printf("[%s] releasing", label)

	releaseStart := time.Now()
	close(release)

	wg.Wait()

	drain := time.Since(releaseStart)

	mu.Lock()
	defer mu.Unlock()

	running := make([]float64, 0, len(*wakes))
	for _, w := range *wakes {
		running = append(running, w.WakeRunningMS)
	}

	sort.Float64s(running)

	res := map[string]any{
		"label":          label,
		"engine":         "hatchet",
		"mode":           "capacity",
		"requested":      n,
		"parked_peak":    peak,
		"parked_at_hold": held,
		"completed":      len(*wakes),
		"failed":         failed.Load(),
		"fill_seconds":   fill.Seconds(),
		"hold_seconds":   holdS,
		"drain_seconds":  drain.Seconds(),
		"wake_p50_ms":    pctF(running, 0.50),
		"wake_p95_ms":    pctF(running, 0.95),
		"wake_max_ms":    lastF(running),
	}

	if e := firstErr.Load(); e != nil {
		res["first_error"] = e
	}

	fmt.Printf("\n=== %s capacity ===\n", label)
	fmt.Printf("requested    %d\n", n)
	fmt.Printf("parked       %d at peak, %d still parked after the hold\n", peak, held)
	fmt.Printf("fill         %.1fs\n", fill.Seconds())
	fmt.Printf("drain        %.1fs after release\n", drain.Seconds())
	fmt.Printf("completed    %d, failed %d\n", len(*wakes), failed.Load())
	fmt.Printf("wake         p50=%.0fms p95=%.0fms max=%.0fms\n",
		res["wake_p50_ms"], res["wake_p95_ms"], res["wake_max_ms"])

	if e := firstErr.Load(); e != nil {
		fmt.Printf("first error  %v\n", e)
	}

	writeResult(res, outPath)
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

	writeResult(res, outPath)
}

func writeResult(res map[string]any, outPath string) {
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

func lastF(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	return sorted[len(sorted)-1]
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
