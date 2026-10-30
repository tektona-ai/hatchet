package nats

import (
	"fmt"
	"strings"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

// defaultInboxPrefix is the reply inbox prefix when ConnConfig.InboxPrefix is
// empty. A private prefix, rather than the shared "_INBOX", lets a server
// permission give Hatchet its own replies and nobody else's (grant
// "_INBOX_hatchet.>").
const defaultInboxPrefix = "_INBOX_hatchet"

// ConnConfig holds the credentials, TLS and inbox settings of a NATS
// connection, on top of the URL and the username and password.
type ConnConfig struct {
	// CredsFile is the path of a NATS credentials file (a user JWT and its nkey
	// seed). nats.go reads it again on every reconnect, so a renewed file takes
	// effect without a restart.
	CredsFile string

	// CAFile is the path of a PEM CA bundle that the server certificate must
	// chain to. Setting it requires TLS. Empty verifies a tls:// server against
	// the system roots.
	CAFile string

	// TLSFirst starts TLS before the client reads the server INFO, so that a
	// TCP or TLS load balancer can sit in front of the server.
	TLSFirst bool

	// AllowPlaintext lets a connection with CredsFile set run without TLS.
	// Without TLS the credentials and every message cross the network in
	// plaintext, so this is for local development and plaintext servers that
	// still wait for their move to TLS.
	AllowPlaintext bool

	// InboxPrefix is the prefix of reply subjects. Empty defaults to
	// "_INBOX_hatchet".
	InboxPrefix string
}

// connectOptions returns the nats.go options shared by the durable queue and
// the pub/sub: retry forever, log connection lifecycle transitions, and apply
// the credentials, TLS and inbox settings of c. It fails when c sends
// credentials over a connection to url that is not TLS, unless
// c.AllowPlaintext is set.
func connectOptions(l *zerolog.Logger, name, url, username, password string, c ConnConfig) ([]natsgo.Option, error) {
	opts := []natsgo.Option{
		// MaxReconnects(-1) retries forever: any finite limit permanently
		// closes the connection once exhausted, leaving the engine without a
		// message queue until a process restart.
		natsgo.MaxReconnects(-1),
		// By default the client closes for good after the server rejects the
		// same credentials twice, whatever MaxReconnects says. A creds file that
		// expired before its renewal landed would then need a restart.
		natsgo.IgnoreAuthErrorAbort(),
		// Empty credentials never reach the wire (the CONNECT payload omits
		// empty user/pass fields), so UserInfo is safe to set unconditionally.
		natsgo.UserInfo(username, password),
		natsgo.CustomInboxPrefix(orDefault(c.InboxPrefix, defaultInboxPrefix)),
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

	if c.CredsFile != "" {
		opts = append(opts, natsgo.UserCredentials(c.CredsFile))
	}

	if c.CAFile != "" {
		opts = append(opts, natsgo.RootCAs(c.CAFile))
	}

	if c.TLSFirst {
		opts = append(opts, natsgo.TLSHandshakeFirst())
	}

	// A CA file and TLS first both make nats.go require TLS on every server.
	if c.CredsFile != "" && !c.AllowPlaintext && c.CAFile == "" && !c.TLSFirst {
		if err := requireTLSURLs(url); err != nil {
			return nil, err
		}
	}

	return opts, nil
}

// requireTLSURLs fails when a server of url is not tls:// or wss://, so that
// it would get the credentials in plaintext.
func requireTLSURLs(url string) error {
	for _, server := range strings.Split(url, ",") {
		server = strings.ToLower(strings.TrimSpace(server))

		if !strings.HasPrefix(server, "tls://") && !strings.HasPrefix(server, "wss://") {
			return fmt.Errorf(
				"nats creds file is set, and %q is not TLS: use a tls:// URL or set a CA file, or allow plaintext for local development only",
				server,
			)
		}
	}

	return nil
}

// newJetStream returns a JetStream client whose API calls go to
// $JS.<domain>.API, or to $JS.API when domain is empty.
func newJetStream(nc *natsgo.Conn, domain string, opts ...jetstream.JetStreamOpt) (jetstream.JetStream, error) {
	if domain == "" {
		return jetstream.New(nc, opts...)
	}

	return jetstream.NewWithDomain(nc, domain, opts...)
}
