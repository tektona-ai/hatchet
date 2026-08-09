package nats

import (
	"crypto/tls"
	"fmt"
	"sort"
	"strings"

	natsgo "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// ConnAuth is the credential material for a NATS connection.
//
// NATS offers several authentication mechanisms and a connection uses exactly
// one. They are kept in a single struct, rather than as loose options, so that
// configuring two at once can be rejected rather than silently resolved by
// whichever the code happens to apply last.
type ConnAuth struct {
	// Username and Password authenticate with a server configured for plain
	// user credentials.
	Username string
	Password string

	// Token authenticates with a server configured for token auth.
	Token string

	// CredentialsFile is a NATS .creds file holding a user JWT and its seed.
	// This is the mechanism operator-issued accounts and Synadia Cloud use.
	CredentialsFile string

	// NKeySeedFile is a file holding an NKey seed, for servers that
	// authenticate NKeys without a JWT.
	NKeySeedFile string
}

// configured names the mechanisms this struct has material for.
func (a ConnAuth) configured() []string {
	var found []string

	if a.Username != "" || a.Password != "" {
		found = append(found, "username/password")
	}

	if a.Token != "" {
		found = append(found, "token")
	}

	if a.CredentialsFile != "" {
		found = append(found, "credentials file")
	}

	if a.NKeySeedFile != "" {
		found = append(found, "nkey seed file")
	}

	sort.Strings(found)

	return found
}

// options converts the credentials into nats.go options.
//
// Configuring more than one mechanism is an error rather than a precedence
// question: silently ignoring credentials an operator deliberately set is how
// a connection ends up authenticating as something other than intended.
func (a ConnAuth) options() ([]natsgo.Option, error) {
	found := a.configured()

	if len(found) > 1 {
		return nil, fmt.Errorf(
			"nats: %d authentication mechanisms configured (%s), but a connection uses exactly one",
			len(found), strings.Join(found, ", "),
		)
	}

	switch {
	case a.CredentialsFile != "":
		return []natsgo.Option{natsgo.UserCredentials(a.CredentialsFile)}, nil

	case a.NKeySeedFile != "":
		opt, err := natsgo.NkeyOptionFromSeed(a.NKeySeedFile)

		if err != nil {
			return nil, fmt.Errorf("nats: could not read nkey seed file %q: %w", a.NKeySeedFile, err)
		}

		return []natsgo.Option{opt}, nil

	case a.Token != "":
		return []natsgo.Option{natsgo.Token(a.Token)}, nil

	default:
		// Empty credentials never reach the wire: the CONNECT payload omits
		// empty user and pass fields, so this is safe to set unconditionally
		// and keeps the no-auth case from needing a branch. Whether a username
		// requires a password is the server's decision.
		return []natsgo.Option{natsgo.UserInfo(a.Username, a.Password)}, nil
	}
}

// connectOptions builds the nats.go options shared by the durable queue and the
// pub/sub: retry forever, log connection lifecycle transitions, and apply
// whichever authentication and TLS the operator configured.
//
// name distinguishes the two connections in logs, since an engine opens both.
func connectOptions(
	l *zerolog.Logger,
	name string,
	auth ConnAuth,
	tlsConfig *tls.Config,
	extra ...natsgo.Option,
) ([]natsgo.Option, error) {
	authOpts, err := auth.options()

	if err != nil {
		return nil, err
	}

	opts := []natsgo.Option{
		// MaxReconnects(-1) retries forever: any finite limit permanently
		// closes the connection once exhausted, leaving the engine without a
		// broker until a process restart.
		natsgo.MaxReconnects(-1),
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

	opts = append(opts, authOpts...)

	if tlsConfig != nil {
		// Secure makes TLS mandatory for this connection: nats.go will not fall
		// back to plaintext if the server does not offer it. That is the point
		// -- an operator who configured TLS should get a failed connection
		// rather than a silently unencrypted one.
		opts = append(opts, natsgo.Secure(tlsConfig))
	}

	return append(opts, extra...), nil
}
