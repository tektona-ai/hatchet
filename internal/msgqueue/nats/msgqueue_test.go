//go:build integration

package nats

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

// newTestMQ builds a MessageQueue namespaced to this test. Streams are a flat
// global namespace and work-queue streams survive the process, so every test
// gets its own stream and subject prefixes and deletes its streams on the way
// out. Without that, a queue name reused across tests would let one test
// consume another's backlog.
func newTestMQ(t testing.TB, opts ...MessageQueueOpt) *MessageQueue {
	t.Helper()

	ns := strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	base := []MessageQueueOpt{
		WithURL(testNATSURL),
		WithStreamPrefix("TEST_" + strings.ToUpper(ns)),
		WithSubjectPrefix("test." + ns),
	}

	cleanup, mq, err := New(append(base, opts...)...)
	require.NoError(t, err)
	require.NotNil(t, mq)

	t.Cleanup(func() {
		deleteStreams(t, "TEST_"+strings.ToUpper(ns))

		if err := cleanup(); err != nil {
			t.Errorf("error cleaning up message queue: %v", err)
		}
	})

	return mq
}

func deleteStreams(t testing.TB, prefix string) {
	t.Helper()

	nc, err := natsgo.Connect(testNATSURL)
	if err != nil {
		return
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for name := range js.StreamNames(ctx).Name() {
		if strings.HasPrefix(name, prefix) {
			_ = js.DeleteStream(ctx, name)
		}
	}
}

func streamNames(t testing.TB, prefix string) []string {
	t.Helper()

	nc, err := natsgo.Connect(testNATSURL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var out []string

	for name := range js.StreamNames(ctx).Name() {
		if strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}

	return out
}

func testMessage(t testing.TB, id string) *msgqueue.Message {
	t.Helper()

	msg, err := msgqueue.NewTenantMessage(uuid.New(), id, false, true, map[string]interface{}{"key": id})
	require.NoError(t, err)

	return msg
}

// waitFor polls until cond holds or the deadline passes, so tests assert on an
// eventual state rather than sleeping for a fixed guess.
func waitFor(t testing.TB, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", msg)
}

func TestSendAndSubscribeRoundtrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := newTestMQ(t)
	q := msgqueue.NewRandomStaticQueue()

	var mu sync.Mutex
	var order []string

	received := make(chan *msgqueue.Message, 1)

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		order = append(order, "pre")
		mu.Unlock()

		return nil
	}, func(m *msgqueue.Message) error {
		mu.Lock()
		order = append(order, "post")
		mu.Unlock()

		received <- m

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	msg := testMessage(t, "task-created")
	require.NoError(t, mq.SendMessage(ctx, q, msg))

	select {
	case got := <-received:
		assert.Equal(t, msg.ID, got.ID)
		assert.Equal(t, msg.TenantID, got.TenantID)
		require.Len(t, got.Payloads, 1)
		assert.Equal(t, msg.Payloads[0], got.Payloads[0])
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"pre", "post"}, order, "pre-ack must run before post-ack")
}

// Several subscribers on one queue must share the work, the way multiple AMQP
// consumers on a single queue do — not each receive a copy.
func TestCompetingConsumersEachMessageOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq := newTestMQ(t)
	q := msgqueue.NewRandomStaticQueue()

	const numMessages = 20

	var mu sync.Mutex
	seen := map[string]int{}

	handler := func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		seen[m.ID]++

		return nil
	}

	for range 3 {
		cleanup, err := mq.Subscribe(q, handler, msgqueue.NoOpHook)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cleanup() })
	}

	for i := range numMessages {
		require.NoError(t, mq.SendMessage(ctx, q, testMessage(t, fmt.Sprintf("msg-%d", i))))
	}

	waitFor(t, 30*time.Second, "all messages to be delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(seen) == numMessages
	})

	mu.Lock()
	defer mu.Unlock()

	for id, count := range seen {
		assert.Equal(t, 1, count, "message %s should be delivered exactly once", id)
	}
}

// A pre-ack failure on a queue with an automatic DLQ must be redelivered after
// the backoff, not dropped and not dead-lettered to a separate queue.
func TestPreAckFailureIsRetriedWithBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	backoff := 500 * time.Millisecond
	mq := newTestMQ(t, WithDeadLetterBackoff(backoff))
	q := msgqueue.NewRandomStaticQueue()

	var mu sync.Mutex
	var attempts int
	var postAcked bool

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()

		if n < 3 {
			return fmt.Errorf("transient failure %d", n)
		}

		return nil
	}, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		postAcked = true

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	require.NoError(t, mq.SendMessage(ctx, q, testMessage(t, "retry-me")))

	waitFor(t, 30*time.Second, "message to succeed after retries", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return postAcked
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 3, attempts, "message should be redelivered until the handler succeeds")
}

// A permanent pre-ack error is a poison message: it must be acked and dropped
// rather than retried forever.
func TestPermanentPreAckErrorDropsMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := newTestMQ(t, WithDeadLetterBackoff(200*time.Millisecond))
	q := msgqueue.NewRandomStaticQueue()

	var mu sync.Mutex
	var attempts int

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++

		return fmt.Errorf("ERROR: invalid input syntax for type json (SQLSTATE 22P02)")
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	require.NoError(t, mq.SendMessage(ctx, q, testMessage(t, "poison")))

	waitFor(t, 15*time.Second, "poison message to be delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return attempts >= 1
	})

	// Well past several backoff windows: a dropped message stays dropped.
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, attempts, "permanent pre-ack failures must not be redelivered")
}

// With message rejection enabled, a message that keeps failing is eventually
// given up on instead of cycling forever.
func TestMessageRejectionStopsRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const maxDeathCount = 2

	mq := newTestMQ(t,
		WithDeadLetterBackoff(200*time.Millisecond),
		WithMessageRejection(true, maxDeathCount),
	)
	q := msgqueue.NewRandomStaticQueue()

	var mu sync.Mutex
	var attempts int

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++

		return fmt.Errorf("always fails")
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	require.NoError(t, mq.SendMessage(ctx, q, testMessage(t, "never-succeeds")))

	// maxDeathCount deliveries invoke the handler; the next one exceeds the
	// limit and is rejected before reaching it.
	waitFor(t, 30*time.Second, "redelivery to stop", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return attempts >= maxDeathCount
	})

	time.Sleep(2 * time.Second)

	mu.Lock()
	assert.LessOrEqual(t, attempts, maxDeathCount+1, "redelivery must stop once the death count is exceeded")
	mu.Unlock()

	// A rejected message on a queue with only an automatic DLQ is dropped, not
	// republished. Republishing would provision a stream for the automatic DLQ
	// that nothing ever consumes, so its backlog would grow without bound.
	assert.NotContains(t, streamNames(t, mq.streamPrefix), mq.streamName(q.DLQ()),
		"rejecting a message must not provision a stream for an automatic DLQ")
}

// A dispatcher queue routes failures straight to its static DLQ, with no retry
// in between, and the DLQ is consumable as an ordinary queue.
func TestDispatcherQueueFailureGoesToDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mq := newTestMQ(t)

	dispatcherQueue := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	require.True(t, dispatcherQueue.IsExpirable())
	require.NotNil(t, dispatcherQueue.DLQ())

	dlqReceived := make(chan *msgqueue.Message, 1)

	cleanupDLQ, err := mq.Subscribe(msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, func(m *msgqueue.Message) error {
		dlqReceived <- m

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanupDLQ() })

	var mu sync.Mutex
	var attempts int

	cleanup, err := mq.Subscribe(dispatcherQueue, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++

		return fmt.Errorf("dispatcher is unhealthy")
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	msg := testMessage(t, "task-assigned")
	require.NoError(t, mq.SendMessage(ctx, dispatcherQueue, msg))

	select {
	case got := <-dlqReceived:
		assert.Equal(t, msg.ID, got.ID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for dead-lettered message")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, attempts, "dispatcher failures dead-letter immediately rather than retrying")
}

// A dispatcher message that outlives its TTL before anyone consumes it is
// dead-lettered on delivery rather than handled, so the scheduler can reassign
// the work.
func TestExpiredDispatcherMessageIsDeadLettered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ttl := time.Second
	mq := newTestMQ(t, WithExpirableMessageTTL(ttl))

	dispatcherQueue := msgqueue.QueueTypeFromDispatcherID(uuid.New())

	dlqReceived := make(chan *msgqueue.Message, 1)

	cleanupDLQ, err := mq.Subscribe(msgqueue.DISPATCHER_DEAD_LETTER_QUEUE, func(m *msgqueue.Message) error {
		dlqReceived <- m

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanupDLQ() })

	msg := testMessage(t, "task-assigned")
	require.NoError(t, mq.SendMessage(ctx, dispatcherQueue, msg))

	// Nothing is consuming the dispatcher queue yet, so the message ages past
	// its TTL exactly as it would while a dispatcher is down.
	time.Sleep(ttl + 500*time.Millisecond)

	var handled bool
	var mu sync.Mutex

	cleanup, err := mq.Subscribe(dispatcherQueue, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		handled = true

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	select {
	case got := <-dlqReceived:
		assert.Equal(t, msg.ID, got.ID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for expired message on the DLQ")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.False(t, handled, "an expired message must not reach the handler")
}

// An oversized multi-payload message is split until each chunk fits, with every
// payload delivered exactly once.
func TestOversizedMessageIsChunked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	mq := newTestMQ(t)
	q := msgqueue.NewRandomStaticQueue()

	// 16 payloads of 1MiB each is ~28MiB on the wire: the []byte is
	// base64-encoded once into the payload JSON and again when the enclosing
	// Message is marshaled. SendMessage must split before anything is accepted.
	const numPayloads = 16

	payloads := make([]map[string]interface{}, numPayloads)
	for i := range payloads {
		b := make([]byte, 1024*1024)
		_, err := rand.Read(b)
		require.NoError(t, err)
		payloads[i] = map[string]interface{}{"data": b}
	}

	msg, err := msgqueue.NewTenantMessage(uuid.New(), "task-stream-event", false, true, payloads...)
	require.NoError(t, err)
	require.Len(t, msg.Payloads, numPayloads)

	var mu sync.Mutex
	var got int
	var chunks int

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		chunks++
		got += len(m.Payloads)

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	require.NoError(t, mq.SendMessage(ctx, q, msg))

	waitFor(t, 60*time.Second, "all chunked payloads", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return got == numPayloads
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, chunks, 1, "message should have been split into multiple chunks")
}

// Compression is a wire concern: an enabled publisher must hand the subscriber
// plain payloads back.
func TestCompressedRoundtrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := newTestMQ(t, WithGzipCompression(true, 1))
	q := msgqueue.NewRandomStaticQueue()

	received := make(chan *msgqueue.Message, 1)

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		received <- m

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	msg := testMessage(t, "compress-me")
	want := make([]byte, len(msg.Payloads[0]))
	copy(want, msg.Payloads[0])

	require.NoError(t, mq.SendMessage(ctx, q, msg))

	select {
	case got := <-received:
		require.Len(t, got.Payloads, 1)
		assert.Equal(t, want, got.Payloads[0], "payload should be transparently decompressed")
		assert.False(t, got.Compressed)
	case <-ctx.Done():
		t.Fatal("timed out waiting for compressed message")
	}

	// The caller's message must not have been mutated into its compressed form:
	// the same *Message is handed to the pub/sub afterwards.
	assert.False(t, msg.Compressed, "SendMessage must not compress the caller's message in place")
	assert.Equal(t, want, msg.Payloads[0])
}

// Clone is used by the OLAP controller to get a second queue with its own QoS.
func TestCloneProducesUsableQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := newTestMQ(t)

	cleanup, clone, err := mq.Clone()
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	clone.SetQOS(10)
	assert.True(t, clone.IsReady())

	q := msgqueue.NewRandomStaticQueue()

	received := make(chan *msgqueue.Message, 1)

	cleanupSub, err := clone.Subscribe(q, func(m *msgqueue.Message) error {
		received <- m

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanupSub() })

	// Published through the original, consumed through the clone: they must
	// address the same streams.
	msg := testMessage(t, "cloned")
	require.NoError(t, mq.SendMessage(ctx, q, msg))

	select {
	case got := <-received:
		assert.Equal(t, msg.ID, got.ID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for message on cloned queue")
	}
}

// Messages published before any subscriber exists must still be delivered:
// durability is the whole point of this backend, and is what separates it from
// the best-effort pub/sub.
func TestMessagesArePersistedBeforeSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mq := newTestMQ(t)
	q := msgqueue.NewRandomStaticQueue()

	const numMessages = 5

	for i := range numMessages {
		require.NoError(t, mq.SendMessage(ctx, q, testMessage(t, fmt.Sprintf("early-%d", i))))
	}

	var mu sync.Mutex
	var got int

	cleanup, err := mq.Subscribe(q, func(m *msgqueue.Message) error {
		mu.Lock()
		defer mu.Unlock()
		got++

		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	waitFor(t, 20*time.Second, "backlog to drain", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return got == numMessages
	})
}

func TestNewRequiresURL(t *testing.T) {
	_, _, err := New()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a URL")
}
