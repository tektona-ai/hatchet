package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/logger"
	"github.com/hatchet-dev/hatchet/pkg/telemetry"
)

const (
	// defaultStreamPrefix prefixes every stream this backend creates. Streams
	// are a flat global namespace per NATS account, so two Hatchet
	// installations sharing an account must differ here (the subject prefix
	// alone is not enough).
	defaultStreamPrefix = "HATCHET"

	// defaultQueueSubjectPrefix is the subject namespace for durable queues. It
	// is deliberately distinct from the pub/sub default ("hatchet.pubsub") so a
	// single NATS server can carry both without subject collisions.
	defaultQueueSubjectPrefix = "hatchet.mq"

	// ackWait mirrors the rabbitmq backend's x-consumer-timeout: a delivery not
	// acked within this window is redelivered.
	ackWait = 5 * time.Minute

	// defaultExpirableMsgTTL mirrors the rabbitmq dispatcher queue's
	// x-message-ttl. An unconsumed dispatcher message older than this is
	// dead-lettered rather than handled.
	defaultExpirableMsgTTL = 20 * time.Second

	// dispatcherConsumerInactiveThreshold mirrors the rabbitmq dispatcher
	// queue's x-expires: the server reaps a dispatcher's consumer this long
	// after it stops pulling, so a dead engine leaves no state behind.
	dispatcherConsumerInactiveThreshold = 10 * time.Minute

	// provisionTimeout bounds the JetStream API calls that create streams and
	// consumers. These are control-plane round trips, not data path.
	provisionTimeout = 30 * time.Second

	mb                       = 1024 * 1024
	maxSizeErrorLogThreshold = 10 * mb
)

// MessageQueue implements msgqueue.MessageQueue over NATS JetStream.
//
// Each Hatchet queue maps to a work-queue-retention stream: messages are
// removed from the stream once acked, so a stream's depth is its backlog, the
// same as an AMQP queue. Non-exclusive queues share one durable consumer
// across every engine replica, which gives the competing-consumer behavior of
// several AMQP consumers on one queue. Exclusive (dispatcher) queues get their
// own filtered consumer on a shared, short-MaxAge stream.
type MessageQueue struct {
	ctx    context.Context
	cancel context.CancelFunc

	nc *natsgo.Conn
	js jetstream.JetStream

	l *zerolog.Logger

	qos int

	subjectPrefix string
	streamPrefix  string

	deadLetterBackoff time.Duration
	expirableMsgTTL   time.Duration

	compressor msgqueue.Compressor

	enableMessageRejection bool
	maxDeathCount          int

	// provisioned memoizes ensureStream so the publish path does not make a
	// JetStream API round trip per message.
	provisioned sync.Map

	configFs []MessageQueueOpt
}

type MessageQueueOpt func(*MessageQueueOpts)

type MessageQueueOpts struct {
	l                      *zerolog.Logger
	url                    string
	username               string
	password               string
	qos                    int
	subjectPrefix          string
	streamPrefix           string
	deadLetterBackoff      time.Duration
	expirableMsgTTL        time.Duration
	compressionEnabled     bool
	compressionThreshold   int
	enableMessageRejection bool
	maxDeathCount          int
}

func defaultMessageQueueOpts() *MessageQueueOpts {
	l := logger.NewDefaultLogger("nats-msgqueue")

	return &MessageQueueOpts{
		l:                      &l,
		qos:                    100,
		deadLetterBackoff:      5 * time.Second,
		expirableMsgTTL:        defaultExpirableMsgTTL,
		enableMessageRejection: false,
		maxDeathCount:          5,
	}
}

// WithURL sets the NATS seed URL(s). Comma-separated lists are passed through
// to nats.go. Prefer bare hosts and set WithUsername/WithPassword so
// rediscovered cluster peers authenticate.
func WithURL(url string) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.url = url
	}
}

func WithUsername(username string) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.username = username
	}
}

func WithPassword(password string) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.password = password
	}
}

// WithSubjectPrefix sets the subject namespace for durable queues (default
// "hatchet.mq"). Empty falls back to the default.
func WithSubjectPrefix(prefix string) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.subjectPrefix = prefix
	}
}

// WithStreamPrefix sets the prefix for created stream names (default
// "HATCHET"). Empty falls back to the default. Streams are global per NATS
// account, so installations sharing an account must set distinct prefixes.
func WithStreamPrefix(prefix string) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.streamPrefix = prefix
	}
}

// WithQos sets the consumer's MaxAckPending, the JetStream analogue of an AMQP
// prefetch count.
func WithQos(qos int) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.qos = qos
	}
}

// WithDeadLetterBackoff sets the redelivery delay applied when a pre-ack
// handler fails on a queue with an automatic DLQ.
func WithDeadLetterBackoff(backoff time.Duration) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.deadLetterBackoff = backoff
	}
}

// WithExpirableMessageTTL sets how long a message on an expirable (dispatcher)
// queue may sit before delivery dead-letters it instead of handling it. It
// mirrors the rabbitmq backend's x-message-ttl; zero or negative keeps the
// default.
func WithExpirableMessageTTL(ttl time.Duration) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		if ttl > 0 {
			opts.expirableMsgTTL = ttl
		}
	}
}

func WithGzipCompression(enabled bool, threshold int) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.compressionEnabled = enabled

		if threshold <= 0 {
			threshold = msgqueue.DefaultCompressionThreshold
		}

		opts.compressionThreshold = threshold
	}
}

// WithMessageRejection caps redelivery attempts. When enabled, a message
// delivered more than maxDeathCount times is terminated (and dead-lettered if
// the queue has a static DLQ) instead of retrying forever.
func WithMessageRejection(enabled bool, maxDeathCount int) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.enableMessageRejection = enabled

		if maxDeathCount <= 0 {
			maxDeathCount = 5
		}

		opts.maxDeathCount = maxDeathCount
	}
}

func WithLogger(l *zerolog.Logger) MessageQueueOpt {
	return func(opts *MessageQueueOpts) {
		opts.l = l
	}
}

// New connects to NATS, verifies JetStream is enabled, and provisions the
// static queues. It fails fast rather than degrading: a misconfigured server
// (JetStream disabled, max_payload too small) is a deployment error, and
// discovering it at the first task publish is far worse than at boot.
func New(fs ...MessageQueueOpt) (func() error, *MessageQueue, error) {
	opts := defaultMessageQueueOpts()

	for _, f := range fs {
		f(opts)
	}

	if opts.url == "" {
		return nil, nil, fmt.Errorf("nats message queue requires a URL to be set")
	}

	newLogger := opts.l.With().Str("service", "nats-msgqueue").Logger()
	l := &newLogger

	nc, err := natsgo.Connect(opts.url, connectOptions(l, "nats msgqueue", opts.username, opts.password)...)

	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to nats at %q: %w", opts.url, err)
	}

	// Same contract as the pub/sub backend: a single payload cannot be chunked,
	// so the server must accept messages up to Hatchet's own limit.
	if nc.MaxPayload() < msgqueue.MaxMessageSize {
		nc.Close()
		return nil, nil, fmt.Errorf(
			"nats server max_payload is %d bytes but hatchet requires at least %d; raise max_payload in the NATS server config",
			nc.MaxPayload(), msgqueue.MaxMessageSize,
		)
	}

	js, err := jetstream.New(nc)

	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("could not initialize jetstream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	t := &MessageQueue{
		ctx:               ctx,
		cancel:            cancel,
		nc:                nc,
		js:                js,
		l:                 l,
		qos:               opts.qos,
		subjectPrefix:     orDefault(opts.subjectPrefix, defaultQueueSubjectPrefix),
		streamPrefix:      orDefault(opts.streamPrefix, defaultStreamPrefix),
		deadLetterBackoff: opts.deadLetterBackoff,
		expirableMsgTTL:   opts.expirableMsgTTL,
		compressor: msgqueue.Compressor{
			Enabled:   opts.compressionEnabled,
			Threshold: opts.compressionThreshold,
		},
		enableMessageRejection: opts.enableMessageRejection,
		maxDeathCount:          opts.maxDeathCount,
		configFs:               fs,
	}

	cleanup := func() error {
		cancel()
		nc.Close()
		return nil
	}

	// Provision the static queues eagerly so a JetStream misconfiguration
	// surfaces at boot rather than on the first publish.
	initCtx, initCancel := context.WithTimeout(ctx, provisionTimeout)
	defer initCancel()

	for _, q := range []msgqueue.Queue{
		msgqueue.TASK_PROCESSING_QUEUE,
		msgqueue.OLAP_QUEUE,
		msgqueue.DISPATCHER_DEAD_LETTER_QUEUE,
	} {
		if err := t.ensureStream(initCtx, q); err != nil {
			_ = cleanup()
			return nil, nil, fmt.Errorf("failed to initialize stream for queue %s: %w", q.Name(), err)
		}
	}

	return cleanup, t, nil
}

func (t *MessageQueue) Clone() (func() error, msgqueue.MessageQueue, error) {
	return New(t.configFs...)
}

func (t *MessageQueue) SetQOS(prefetchCount int) {
	t.qos = prefetchCount
}

func (t *MessageQueue) IsReady() bool {
	return t.nc.IsConnected()
}

// subject returns the NATS subject a queue's messages are published to.
// Expirable (dispatcher) queues live under a separate token so they can share
// one short-MaxAge stream via a wildcard.
func (t *MessageQueue) subject(q msgqueue.Queue) string {
	if q.IsExpirable() {
		return fmt.Sprintf("%s.dispatcher.%s", t.subjectPrefix, q.Name())
	}

	return fmt.Sprintf("%s.q.%s", t.subjectPrefix, q.Name())
}

// streamName returns the stream backing a queue. Every dispatcher queue shares
// a single stream: they are created and destroyed with engine processes, and a
// stream per dispatcher would churn JetStream metadata for no benefit.
func (t *MessageQueue) streamName(q msgqueue.Queue) string {
	if q.IsExpirable() {
		return t.streamPrefix + "_DISPATCHER"
	}

	return t.streamPrefix + "_" + sanitizeName(q.Name())
}

// streamConfig describes the stream backing a queue.
//
// Retention is WorkQueuePolicy so an acked message is removed immediately and
// the stream's depth is the queue backlog. That policy forbids overlapping
// consumer filters, which is exactly the invariant Hatchet wants: one logical
// consumer per queue, shared by every replica.
func (t *MessageQueue) streamConfig(q msgqueue.Queue) jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:      t.streamName(q),
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		// DiscardNew fails the publish when a stream hits its limits instead of
		// evicting the oldest messages. For a task queue a rejected enqueue is
		// an error the caller can see and retry, whereas a silently dropped
		// backlog entry is a task that never runs and is never reported.
		Discard: jetstream.DiscardNew,
		// Hatchet chunks anything larger before it reaches the wire, so a
		// message above this is a bug rather than a load condition.
		MaxMsgSize: int32(msgqueue.MaxMessageSize),
	}

	if q.IsExpirable() {
		cfg.Subjects = []string{fmt.Sprintf("%s.dispatcher.*", t.subjectPrefix)}
		// MaxAge is only a storage backstop for dispatcher messages that no
		// consumer ever receives. The per-message TTL contract is enforced on
		// delivery instead (expired messages are dead-lettered), so MaxAge is
		// set well above it, to the window after which the server would have
		// reaped the owning consumer anyway. Ageing a message out here is the
		// same outcome AMQP gives when an exclusive dispatcher queue dies with
		// its connection.
		cfg.MaxAge = dispatcherConsumerInactiveThreshold
	} else {
		cfg.Subjects = []string{t.subject(q)}
	}

	return cfg
}

// ensureStream creates or updates the stream backing a queue, memoizing
// success so the publish path stays free of JetStream API round trips.
func (t *MessageQueue) ensureStream(ctx context.Context, q msgqueue.Queue) error {
	name := t.streamName(q)

	if _, ok := t.provisioned.Load(name); ok {
		return nil
	}

	if _, err := t.js.CreateOrUpdateStream(ctx, t.streamConfig(q)); err != nil {
		return fmt.Errorf("could not create or update stream %s: %w", name, err)
	}

	t.provisioned.Store(name, struct{}{})

	return nil
}

func (t *MessageQueue) SendMessage(ctx context.Context, q msgqueue.Queue, msg *msgqueue.Message) error {
	ctx, span := telemetry.NewSpan(ctx, "MessageQueue.SendMessage")
	defer span.End()

	span.SetAttributes(
		attribute.String("MessageQueue.SendMessage.queue_name", q.Name()),
		attribute.String("MessageQueue.SendMessage.tenant_id", msg.TenantID.String()),
		attribute.String("MessageQueue.SendMessage.message_id", msg.ID),
		attribute.Int("MessageQueue.SendMessage.num_payloads", len(msg.Payloads)),
	)

	if err := t.pubMessage(ctx, q, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "error publishing message")
		return err
	}

	return nil
}

func (t *MessageQueue) pubMessage(ctx context.Context, q msgqueue.Queue, msg *msgqueue.Message) error {
	otelCarrier := telemetry.GetCarrier(ctx)

	ctx, span := telemetry.NewSpanWithCarrier(ctx, "publish-message", otelCarrier)
	defer span.End()

	telemetry.WithAttributes(span, telemetry.AttributeKV{Key: "tenant.id", Value: msg.TenantID})

	msg.SetOtelCarrier(otelCarrier)

	var compressionResult *msgqueue.CompressionResult

	// Work on a shallow copy rather than mutating in place: callers may hand
	// the same *Message to a pub/sub on a different backend afterwards
	// (PubTenantMessage), and compression is a wire concern of this backend.
	if len(msg.Payloads) > 0 && !msg.Compressed {
		var err error
		compressionResult, err = t.compressor.CompressPayloads(msg.Payloads)

		if err != nil {
			t.l.Error().Ctx(ctx).Err(err).Msg("error compressing payloads")
			return fmt.Errorf("failed to compress payloads: %w", err)
		}

		if compressionResult.WasCompressed {
			msgCp := msg.Clone()
			msgCp.Payloads = compressionResult.Payloads
			msgCp.Compressed = true
			msg = msgCp
		}
	}

	if err := t.ensureStream(ctx, q); err != nil {
		return err
	}

	body, err := json.Marshal(msg)

	if err != nil {
		t.l.Error().Ctx(ctx).Err(err).Msg("error marshaling message")
		return err
	}

	// The stream rejects anything above MaxMsgSize and the connection rejects
	// anything above max_payload, so honour whichever binds first.
	maxSize := int(min(int64(msgqueue.MaxMessageSize), t.nc.MaxPayload()))

	if len(body) > maxSize {
		if len(msg.Payloads) == 1 {
			err := fmt.Errorf("message size %d bytes exceeds maximum allowed size of %d bytes", len(body), maxSize)
			span.RecordError(err)
			span.SetStatus(codes.Error, "message size exceeds maximum allowed size")
			return err
		}

		// Split the payloads in half and publish recursively until every chunk
		// fits, same strategy as the rabbitmq and nats pub/sub backends.
		payloadsPerChunk := max(len(msg.Payloads)/2, 1)

		for chunk := range slices.Chunk(msg.Payloads, payloadsPerChunk) {
			msgCp := msg.Clone()
			msgCp.Payloads = chunk

			if err := t.pubMessage(ctx, q, msgCp); err != nil {
				return err
			}
		}

		return nil
	}

	if len(body) > maxSizeErrorLogThreshold {
		t.l.Error().Ctx(ctx).
			Int("message_size_bytes", len(body)).
			Int("num_messages", len(msg.Payloads)).
			Str("tenant_id", msg.TenantID.String()).
			Str("queue_name", q.Name()).
			Str("message_id", msg.ID).
			Msg("sending a very large message, this may impact performance")
	}

	subject := t.subject(q)

	ctx, pubSpan := telemetry.NewSpan(ctx, "publish_message")
	defer pubSpan.End()

	spanAttrs := []attribute.KeyValue{
		attribute.String("MessageQueue.publish_message.queue_name", q.Name()),
		attribute.String("MessageQueue.publish_message.tenant_id", msg.TenantID.String()),
		attribute.String("MessageQueue.publish_message.message_id", msg.ID),
	}

	if compressionResult != nil && compressionResult.WasCompressed {
		spanAttrs = append(spanAttrs,
			attribute.Bool("MessageQueue.publish_message.compressed", true),
			attribute.Int("MessageQueue.publish_message.original_size", compressionResult.OriginalSize),
			attribute.Int("MessageQueue.publish_message.compressed_size", compressionResult.CompressedSize),
			attribute.Float64("MessageQueue.publish_message.compression_ratio", compressionResult.CompressionRatio),
		)
	}

	pubSpan.SetAttributes(spanAttrs...)

	// Unlike the rabbitmq backend, which publishes without confirms, JetStream
	// acks every publish. A nil error here means the message is persisted and
	// replicated, so callers get a real durability guarantee.
	if _, err := t.js.Publish(ctx, subject, body); err != nil {
		pubSpan.RecordError(err)
		pubSpan.SetStatus(codes.Error, "error publishing message")
		return fmt.Errorf("could not publish to %s: %w", subject, err)
	}

	return nil
}

// Subscribe consumes a queue until the returned cleanup function is called.
//
// Unlike the rabbitmq backend there is no companion subscription to an
// automatic DLQ: JetStream expresses retry-with-backoff directly through
// NakWithDelay, so failed messages are redelivered on the same consumer rather
// than being parked in a pair of ping-ponging dead-letter queues.
func (t *MessageQueue) Subscribe(q msgqueue.Queue, preAck msgqueue.MsgHandler, postAck msgqueue.MsgHandler) (func() error, error) {
	ctx, cancel := context.WithCancel(t.ctx)

	provisionCtx, provisionCancel := context.WithTimeout(ctx, provisionTimeout)
	defer provisionCancel()

	if err := t.ensureStream(provisionCtx, q); err != nil {
		cancel()
		return nil, err
	}

	cons, err := t.js.CreateOrUpdateConsumer(provisionCtx, t.streamName(q), t.consumerConfig(q))

	if err != nil {
		cancel()
		return nil, fmt.Errorf("could not create consumer for queue %s: %w", q.Name(), err)
	}

	// wg tracks in-flight handler goroutines so cleanup can drain them. The
	// number in flight is bounded by MaxAckPending, since a message is only
	// delivered once the consumer has ack capacity for it.
	//
	// stopMu guards the wg.Add against the wg.Wait in cleanup: Drain lets
	// already-buffered messages through, so without the flag a delivery could
	// call Add on a zero counter that Wait is already blocked on, which is a
	// data race. A message refused here is simply left unacked and redelivered.
	wg := &sync.WaitGroup{}
	stopMu := &sync.Mutex{}
	stopped := false

	consumeCtx, err := cons.Consume(func(natsMsg jetstream.Msg) {
		stopMu.Lock()

		if stopped {
			stopMu.Unlock()
			return
		}

		wg.Add(1)
		stopMu.Unlock()

		go func() {
			defer wg.Done()
			t.handleDelivery(ctx, q, natsMsg, preAck, postAck)
		}()
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// Transient pull errors are recovered by nats.go internally; surface
		// them so a persistently broken consumer is visible.
		if ctx.Err() != nil {
			return
		}

		t.l.Error().Err(err).Str("queue", q.Name()).Msg("nats msgqueue consume error")
	}))

	if err != nil {
		cancel()
		return nil, fmt.Errorf("could not consume queue %s: %w", q.Name(), err)
	}

	return func() error {
		// Drain stops the pull loop but lets already-buffered messages through,
		// so refuse new work before waiting on what is already running.
		consumeCtx.Drain()

		stopMu.Lock()
		stopped = true
		stopMu.Unlock()

		wg.Wait()
		// Cancel last: handlers use this context to dead-letter, and cancelling
		// it while they run would fail those publishes for no reason.
		cancel()

		return nil
	}, nil
}

func (t *MessageQueue) consumerConfig(q msgqueue.Queue) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxAckPending: t.qos,
		FilterSubject: t.subject(q),
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// -1 is unlimited: without message rejection a failing message retries
		// forever with backoff, matching the rabbitmq automatic-DLQ loop.
		MaxDeliver: -1,
	}

	if t.enableMessageRejection {
		// +1 because the check in handleDelivery rejects strictly above
		// maxDeathCount, so the server should allow that final delivery
		// through for the handler to observe and dead-letter it explicitly.
		cfg.MaxDeliver = t.maxDeathCount + 1
	}

	name := sanitizeName(q.Name())

	if q.Exclusive() {
		// Exclusive queues belong to one process. Name (not Durable) plus an
		// inactive threshold gives an ephemeral consumer the server reaps once
		// the owning engine stops pulling, which is how the AMQP backend's
		// exclusive queue disappears with its connection.
		cfg.Name = name
		cfg.InactiveThreshold = dispatcherConsumerInactiveThreshold
	} else {
		// Every replica shares one durable consumer, giving the competing-
		// consumer semantics of several AMQP consumers on a single queue.
		cfg.Durable = name
	}

	return cfg
}

// handleDelivery runs one message through the pre-ack/ack/post-ack sequence.
//
// Ack semantics mirror the rabbitmq backend: a malformed message is dropped, a
// permanent pre-ack failure is dropped, and any other pre-ack failure is
// retried (or dead-lettered, for queues with a static DLQ).
func (t *MessageQueue) handleDelivery(
	ctx context.Context,
	q msgqueue.Queue,
	natsMsg jetstream.Msg,
	preAck msgqueue.MsgHandler,
	postAck msgqueue.MsgHandler,
) {
	data := natsMsg.Data()

	if len(data) == 0 {
		t.l.Error().Str("queue", q.Name()).Msg("empty message body")
		t.term(natsMsg, "empty message body")

		return
	}

	msg := &msgqueue.Message{}

	if err := json.Unmarshal(data, msg); err != nil {
		t.l.Error().Err(err).Str("queue", q.Name()).Msg("error unmarshalling message")
		// A message that cannot be parsed will never parse. Terminating is the
		// only way to keep it from occupying redelivery capacity forever.
		t.term(natsMsg, "unparseable message")

		return
	}

	if msg.Compressed {
		decompressed, err := msgqueue.DecompressPayloads(msg.Payloads)

		if err != nil {
			t.l.Error().Err(err).Str("message_id", msg.ID).Msg("error decompressing payloads")
			t.term(natsMsg, "undecompressable payloads")

			return
		}

		msg.Payloads = decompressed
		msg.Compressed = false
	}

	md, err := natsMsg.Metadata()

	if err != nil {
		t.l.Error().Err(err).Str("message_id", msg.ID).Msg("error reading message metadata")
		t.term(natsMsg, "unreadable metadata")

		return
	}

	// Expirable queues carry a per-message TTL. The AMQP backend has the broker
	// enforce it via x-message-ttl plus a dead-letter exchange; JetStream has no
	// equivalent, so the deadline is enforced here on delivery instead.
	if q.IsExpirable() && time.Since(md.Timestamp) > t.expirableMsgTTL {
		t.l.Warn().
			Str("message_id", msg.ID).
			Str("queue", q.Name()).
			Dur("age", time.Since(md.Timestamp)).
			Msg("dead-lettering expired message")

		t.deadLetterAndAck(ctx, q, natsMsg, msg)

		return
	}

	// NumDelivered is JetStream's redelivery counter, the analogue of the AMQP
	// x-death count header.
	deathCount := md.NumDelivered

	if deathCount > 5 {
		t.l.Error().
			Uint64("death_count", deathCount).
			Str("message_id", msg.ID).
			Str("tenant_id", msg.TenantID.String()).
			Int("num_payloads", len(msg.Payloads)).
			Msgf("message has been retried for %d times", deathCount)
	}

	if t.enableMessageRejection && deathCount > uint64(t.maxDeathCount) { // nolint: gosec
		t.l.Error().
			Uint64("death_count", deathCount).
			Str("message_id", msg.ID).
			Str("tenant_id", msg.TenantID.String()).
			Int("max_death_count", t.maxDeathCount).
			Msg("permanently rejecting message due to exceeding max death count")

		t.deadLetterAndAck(ctx, q, natsMsg, msg)

		return
	}

	if err := preAck(msg); err != nil {
		if msgqueue.IsPermanentPreAckError(err) {
			t.l.Error().
				Err(err).
				Str("message_id", msg.ID).
				Str("tenant_id", msg.TenantID.String()).
				Int("num_payloads", len(msg.Payloads)).
				Msg("dropping message due to permanent pre-ack error")

			t.ack(natsMsg, msg.ID)

			return
		}

		t.l.Error().Err(err).Str("message_id", msg.ID).Msg("error in pre-ack")

		// A queue with a static DLQ hands failures straight over to it, the way
		// a rejected AMQP message is routed by the dead-letter exchange with no
		// retry in between. Everything else retries in place after a backoff.
		if hasStaticDLQ(q) {
			t.deadLetterAndAck(ctx, q, natsMsg, msg)

			return
		}

		if err := natsMsg.NakWithDelay(t.deadLetterBackoff); err != nil {
			t.l.Error().Err(err).Str("message_id", msg.ID).Msg("error nacking message")
		}

		return
	}

	if !t.ack(natsMsg, msg.ID) {
		// The message will be redelivered after AckWait. Skipping post-ack
		// keeps its side effects paired with a successful ack.
		return
	}

	if err := postAck(msg); err != nil {
		t.l.Error().Err(err).Str("message_id", msg.ID).Msg("error in post-ack")
	}
}

func (t *MessageQueue) ack(natsMsg jetstream.Msg, msgID string) bool {
	if err := natsMsg.Ack(); err != nil {
		t.l.Error().Err(err).Str("message_id", msgID).Msg("error acknowledging message")
		return false
	}

	return true
}

// term tells the server never to redeliver this message. It is reserved for
// messages no retry could fix.
func (t *MessageQueue) term(natsMsg jetstream.Msg, reason string) {
	if err := natsMsg.Term(); err != nil {
		t.l.Error().Err(err).Str("reason", reason).Msg("error terminating message")
	}
}

// deadLetterAndAck takes a message off this queue for good: republished to the
// queue's static DLQ when it has one, dropped otherwise.
//
// JetStream has no dead-letter exchange, so the hop is performed client-side.
// The ack happens only once the republish has succeeded: acking first would
// lose the message outright if the DLQ publish failed, whereas leaving it
// unacked means AckWait redelivers it and the hop is retried.
//
// A queue whose only DLQ is automatic has nowhere to route to — the automatic
// DLQ *is* the retry loop, and republishing into it would pile messages into a
// stream no one consumes. Such a message is dropped instead, matching the
// rabbitmq backend, which acks a message past the max death count.
func (t *MessageQueue) deadLetterAndAck(ctx context.Context, q msgqueue.Queue, natsMsg jetstream.Msg, msg *msgqueue.Message) {
	if !hasStaticDLQ(q) {
		t.l.Warn().
			Str("message_id", msg.ID).
			Str("queue", q.Name()).
			Msg("dropping message: queue has no static dead letter queue")

		t.ack(natsMsg, msg.ID)

		return
	}

	dlq := q.DLQ()

	if err := t.SendMessage(ctx, dlq, msg); err != nil {
		t.l.Error().Err(err).
			Str("message_id", msg.ID).
			Str("dlq", dlq.Name()).
			Msg("error dead-lettering message, leaving unacked for redelivery")

		return
	}

	t.ack(natsMsg, msg.ID)
}

// hasStaticDLQ reports whether failures on this queue should be routed to a
// separate dead-letter queue rather than retried in place. Automatic DLQs are
// the retry loop itself, which NakWithDelay expresses directly.
func hasStaticDLQ(q msgqueue.Queue) bool {
	dlq := q.DLQ()

	return dlq != nil && !dlq.IsAutoDLQ()
}

// connectOptions returns the nats.go options shared by the durable queue and
// the pub/sub: retry forever, and log connection lifecycle transitions.
func connectOptions(l *zerolog.Logger, name, username, password string) []natsgo.Option {
	return []natsgo.Option{
		// MaxReconnects(-1) retries forever: any finite limit permanently
		// closes the connection once exhausted, leaving the engine without a
		// message queue until a process restart.
		natsgo.MaxReconnects(-1),
		// Empty credentials never reach the wire (the CONNECT payload omits
		// empty user/pass fields), so UserInfo is safe to set unconditionally.
		natsgo.UserInfo(username, password),
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			l.Warn().Err(err).Msgf("%s disconnected", name)
		}),
		natsgo.ReconnectHandler(func(nc *natsgo.Conn) {
			l.Info().Str("url", nc.ConnectedUrl()).Msgf("%s reconnected", name)
		}),
		natsgo.ClosedHandler(func(_ *natsgo.Conn) {
			l.Info().Msgf("%s connection closed", name)
		}),
		natsgo.ErrorHandler(func(_ *natsgo.Conn, sub *natsgo.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			l.Error().Err(err).Str("subject", subject).Msgf("%s async error", name)
		}),
	}
}

// sanitizeName maps a Hatchet queue name onto the character set NATS allows for
// stream and consumer names, which excludes ".", "*", ">", whitespace and path
// separators. Queue names are generated from UUIDs and fixed literals, so this
// is a guard against future name shapes rather than a transformation that fires
// today; it must stay injective enough not to collide two live queues.
func sanitizeName(name string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>', '/', '\\', ' ', '\t', '\n':
			return '_'
		}

		return r
	}, name)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}
