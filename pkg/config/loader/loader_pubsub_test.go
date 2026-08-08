package loader

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPubSubSettingsInheritance checks the config backwards-compatibility
// invariant: a deployment configured only with today's durable message queue
// variables (including the legacy SERVER_TASKQUEUE_* aliases) resolves to a
// fully configured pub/sub path with zero new variables.
func TestPubSubSettingsInheritance(t *testing.T) {
	cases := []struct {
		name                  string
		env                   map[string]string
		wantKind              string
		wantURL               string
		wantNatsURL           string
		wantNatsUsername      string
		wantNatsPassword      string
		wantNatsSubjectPrefix string
		wantMaxPub            int32
		wantMaxSub            int32
	}{
		{
			name: "modern rabbit env only",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":         "rabbitmq",
				"SERVER_MSGQUEUE_RABBITMQ_URL": "amqp://user:password@rabbit:5672/",
			},
			wantKind:   "rabbitmq",
			wantURL:    "amqp://user:password@rabbit:5672/",
			wantMaxPub: 10,
			wantMaxSub: 20,
		},
		{
			name: "legacy taskqueue aliases only",
			env: map[string]string{
				"SERVER_TASKQUEUE_KIND":         "rabbitmq",
				"SERVER_TASKQUEUE_RABBITMQ_URL": "amqp://legacy:password@rabbit:5672/",
			},
			wantKind:   "rabbitmq",
			wantURL:    "amqp://legacy:password@rabbit:5672/",
			wantMaxPub: 10,
			wantMaxSub: 20,
		},
		{
			name: "postgres durable only",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND": "postgres",
			},
			wantKind:   "postgres",
			wantURL:    "",
			wantMaxPub: 10,
			wantMaxSub: 20,
		},
		{
			name: "explicit pubsub overrides",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":                          "postgres",
				"SERVER_MSGQUEUE_PUBSUB_KIND":                   "rabbitmq",
				"SERVER_MSGQUEUE_PUBSUB_RABBITMQ_URL":           "amqp://pubsub:password@rabbit:5672/",
				"SERVER_MSGQUEUE_PUBSUB_RABBITMQ_MAX_PUB_CHANS": "3",
				"SERVER_MSGQUEUE_PUBSUB_RABBITMQ_MAX_SUB_CHANS": "7",
			},
			wantKind:   "rabbitmq",
			wantURL:    "amqp://pubsub:password@rabbit:5672/",
			wantMaxPub: 3,
			wantMaxSub: 7,
		},
		{
			name: "nats pubsub with rabbit durable",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":            "rabbitmq",
				"SERVER_MSGQUEUE_RABBITMQ_URL":    "amqp://user:password@rabbit:5672/",
				"SERVER_MSGQUEUE_PUBSUB_KIND":     "nats",
				"SERVER_MSGQUEUE_PUBSUB_NATS_URL": "nats://nats:4222",
			},
			wantKind:    "nats",
			wantURL:     "amqp://user:password@rabbit:5672/",
			wantNatsURL: "nats://nats:4222",
			wantMaxPub:  10,
			wantMaxSub:  20,
		},
		{
			name: "nats pubsub with postgres durable",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":            "postgres",
				"SERVER_MSGQUEUE_PUBSUB_KIND":     "nats",
				"SERVER_MSGQUEUE_PUBSUB_NATS_URL": "nats://127.0.0.1:4222,nats://127.0.0.1:4223",
			},
			wantKind:    "nats",
			wantURL:     "",
			wantNatsURL: "nats://127.0.0.1:4222,nats://127.0.0.1:4223",
			wantMaxPub:  10,
			wantMaxSub:  20,
		},
		{
			name: "nats pubsub with username and password",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":                 "rabbitmq",
				"SERVER_MSGQUEUE_RABBITMQ_URL":         "amqp://user:password@rabbit:5672/",
				"SERVER_MSGQUEUE_PUBSUB_KIND":          "nats",
				"SERVER_MSGQUEUE_PUBSUB_NATS_URL":      "nats://nats:4222",
				"SERVER_MSGQUEUE_PUBSUB_NATS_USERNAME": "hatchet",
				"SERVER_MSGQUEUE_PUBSUB_NATS_PASSWORD": "s3cret",
			},
			wantKind:         "nats",
			wantURL:          "amqp://user:password@rabbit:5672/",
			wantNatsURL:      "nats://nats:4222",
			wantNatsUsername: "hatchet",
			wantNatsPassword: "s3cret",
			wantMaxPub:       10,
			wantMaxSub:       20,
		},
		{
			name: "nats pubsub with subject prefix",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":                       "rabbitmq",
				"SERVER_MSGQUEUE_RABBITMQ_URL":               "amqp://user:password@rabbit:5672/",
				"SERVER_MSGQUEUE_PUBSUB_KIND":                "nats",
				"SERVER_MSGQUEUE_PUBSUB_NATS_URL":            "nats://nats:4222",
				"SERVER_MSGQUEUE_PUBSUB_NATS_SUBJECT_PREFIX": "my.prefix",
			},
			wantKind:              "nats",
			wantURL:               "amqp://user:password@rabbit:5672/",
			wantNatsURL:           "nats://nats:4222",
			wantNatsSubjectPrefix: "my.prefix",
			wantMaxPub:            10,
			wantMaxSub:            20,
		},
		{
			// A NATS-only deployment sets the durable kind and nothing else;
			// the pub/sub must follow it rather than demanding its own block.
			name: "nats durable only",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":     "nats",
				"SERVER_MSGQUEUE_NATS_URL": "nats://nats:4222",
			},
			wantKind:   "nats",
			wantURL:    "",
			wantMaxPub: 10,
			wantMaxSub: 20,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cf, err := LoadServerConfigFile()
			require.NoError(t, err)

			kind, url := resolvePubSubKindAndURL(cf)

			assert.Equal(t, tc.wantKind, kind)
			assert.Equal(t, tc.wantURL, url)
			assert.Equal(t, tc.wantMaxPub, cf.MessageQueue.PubSub.RabbitMQ.MaxPubChans)
			assert.Equal(t, tc.wantMaxSub, cf.MessageQueue.PubSub.RabbitMQ.MaxSubChans)
			assert.Equal(t, tc.wantNatsURL, cf.MessageQueue.PubSub.NATS.URL)
			assert.Equal(t, tc.wantNatsUsername, cf.MessageQueue.PubSub.NATS.Username)
			assert.Equal(t, tc.wantNatsPassword, cf.MessageQueue.PubSub.NATS.Password)
			assert.Equal(t, tc.wantNatsSubjectPrefix, cf.MessageQueue.PubSub.NATS.SubjectPrefix)
		})
	}
}

// TestPubSubNATSConnInheritance checks that a NATS-only deployment configures
// the pub/sub from the durable queue's connection details, and that an explicit
// pub/sub URL is never mixed with the durable queue's credentials.
func TestPubSubNATSConnInheritance(t *testing.T) {
	cases := []struct {
		name         string
		env          map[string]string
		wantURL      string
		wantUsername string
		wantPassword string
	}{
		{
			name: "inherits durable nats connection",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":          "nats",
				"SERVER_MSGQUEUE_NATS_URL":      "nats://nats:4222",
				"SERVER_MSGQUEUE_NATS_USERNAME": "hatchet",
				"SERVER_MSGQUEUE_NATS_PASSWORD": "s3cret",
			},
			wantURL:      "nats://nats:4222",
			wantUsername: "hatchet",
			wantPassword: "s3cret",
		},
		{
			// An explicit pub/sub URL takes the pub/sub's own credentials, even
			// when they are empty: silently pairing it with the durable queue's
			// user and password would invent a credential pair.
			name: "explicit pubsub url does not inherit credentials",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":                 "nats",
				"SERVER_MSGQUEUE_NATS_URL":             "nats://durable:4222",
				"SERVER_MSGQUEUE_NATS_USERNAME":        "durable-user",
				"SERVER_MSGQUEUE_NATS_PASSWORD":        "durable-pass",
				"SERVER_MSGQUEUE_PUBSUB_NATS_URL":      "nats://pubsub:4222",
				"SERVER_MSGQUEUE_PUBSUB_KIND":          "nats",
				"SERVER_MSGQUEUE_PUBSUB_NATS_USERNAME": "pubsub-user",
			},
			wantURL:      "nats://pubsub:4222",
			wantUsername: "pubsub-user",
			wantPassword: "",
		},
		{
			// The durable queue is not NATS, so there is nothing to inherit.
			name: "no inheritance from a non-nats durable queue",
			env: map[string]string{
				"SERVER_MSGQUEUE_KIND":         "rabbitmq",
				"SERVER_MSGQUEUE_RABBITMQ_URL": "amqp://user:password@rabbit:5672/",
				"SERVER_MSGQUEUE_PUBSUB_KIND":  "nats",
			},
			wantURL:      "",
			wantUsername: "",
			wantPassword: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cf, err := LoadServerConfigFile()
			require.NoError(t, err)

			url, username, password := resolvePubSubNATSConn(cf)

			assert.Equal(t, tc.wantURL, url)
			assert.Equal(t, tc.wantUsername, username)
			assert.Equal(t, tc.wantPassword, password)
		})
	}
}

// A durable queue on NATS with no URL is a misconfiguration, not something to
// fall back from.
func TestMsgQueueNATSMissingURLRejected(t *testing.T) {
	t.Setenv("SERVER_MSGQUEUE_KIND", "nats")
	// intentionally omit SERVER_MSGQUEUE_NATS_URL

	cf, err := LoadServerConfigFile()
	require.NoError(t, err)

	assert.Equal(t, "nats", cf.MessageQueue.Kind)
	assert.Empty(t, cf.MessageQueue.NATS.URL)
}

func TestPubSubNATSMissingURLRejected(t *testing.T) {
	t.Setenv("SERVER_MSGQUEUE_KIND", "rabbitmq")
	t.Setenv("SERVER_MSGQUEUE_RABBITMQ_URL", "amqp://user:password@rabbit:5672/")
	t.Setenv("SERVER_MSGQUEUE_PUBSUB_KIND", "nats")
	// intentionally omit SERVER_MSGQUEUE_PUBSUB_NATS_URL

	cf, err := LoadServerConfigFile()
	require.NoError(t, err)

	l := zerolog.Nop()
	_, _, err = createPubSubV1(nil, cf, &l)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a URL")
}
