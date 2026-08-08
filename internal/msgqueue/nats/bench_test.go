//go:build integration

package nats

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

// These benchmarks isolate the durable queue from the rest of the engine, so a
// change here can be evaluated without standing up Postgres, the scheduler and
// a worker. They require a JetStream server at testNATSURL.

// BenchmarkSendMessage measures publish throughput with no consumer attached.
// This is the path the gRPC ingest hits on every task enqueue.
func BenchmarkSendMessage(b *testing.B) {
	mq := newTestMQ(b)
	q := msgqueue.NewRandomStaticQueue()
	msg := testMessage(b, "bench-publish")

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := mq.SendMessage(ctx, q, msg); err != nil {
				b.Fatalf("send failed: %v", err)
			}
		}
	})

	b.StopTimer()
	reportRate(b, "msgs/s")
}

// BenchmarkSendMessageSerial measures single-producer publish latency, which is
// what a caller publishing one message at a time actually experiences.
func BenchmarkSendMessageSerial(b *testing.B) {
	mq := newTestMQ(b)
	q := msgqueue.NewRandomStaticQueue()
	msg := testMessage(b, "bench-publish-serial")

	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		if err := mq.SendMessage(ctx, q, msg); err != nil {
			b.Fatalf("send failed: %v", err)
		}
	}

	b.StopTimer()
	reportRate(b, "msgs/s")
}

// BenchmarkSendMessageSerialAsync is BenchmarkSendMessageSerial with async
// publishing: the same serial caller, without a round trip per message.
func BenchmarkSendMessageSerialAsync(b *testing.B) {
	mq := newTestMQ(b, WithAsyncPublish(true))
	q := msgqueue.NewRandomStaticQueue()
	msg := testMessage(b, "bench-publish-serial-async")

	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		if err := mq.SendMessage(ctx, q, msg); err != nil {
			b.Fatalf("send failed: %v", err)
		}
	}

	b.StopTimer()
	reportRate(b, "msgs/s")
}

// BenchmarkDrain measures end-to-end queue throughput: publish a backlog, then
// subscribe and time how long the consumer takes to work through it. This is
// the number the engine-level benchmark is dominated by.
func BenchmarkDrain(b *testing.B) {
	mq := newTestMQ(b)
	q := msgqueue.NewRandomStaticQueue()
	msg := testMessage(b, "bench-drain")

	ctx := context.Background()

	// Fill the backlog before timing: publishing is measured separately, and
	// including it here would make this a publish benchmark again.
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range b.N / 8 {
				if err := mq.SendMessage(ctx, q, msg); err != nil {
					b.Errorf("send failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	target := int64(b.N/8) * 8
	if target == 0 {
		return
	}

	var got atomic.Int64

	done := make(chan struct{})
	var once sync.Once

	b.ResetTimer()

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		if got.Add(1) >= target {
			once.Do(func() { close(done) })
		}

		return nil
	}, msgqueue.NoOpHook)
	if err != nil {
		b.Fatalf("subscribe failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(120 * time.Second):
		b.Fatalf("timed out draining: %d/%d", got.Load(), target)
	}

	b.StopTimer()
	_ = cleanup()

	reportRate(b, "msgs/s")
}

// reportRate turns Go's ns/op into the throughput figure these comparisons are
// actually about.
func reportRate(b *testing.B, unit string) {
	b.Helper()

	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, unit)
	}
}
