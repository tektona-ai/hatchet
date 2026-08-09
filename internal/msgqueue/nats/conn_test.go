package nats

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	natsgo "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Configuring two authentication mechanisms is rejected rather than resolved by
// precedence. Silently ignoring credentials an operator set is how a connection
// ends up authenticating as something other than intended.
func TestConnAuthRejectsMultipleMechanisms(t *testing.T) {
	cases := []struct {
		name string
		auth ConnAuth
	}{
		{
			name: "password and token",
			auth: ConnAuth{Username: "u", Password: "p", Token: "t"},
		},
		{
			name: "token and credentials file",
			auth: ConnAuth{Token: "t", CredentialsFile: "/tmp/x.creds"},
		},
		{
			name: "credentials file and nkey seed",
			auth: ConnAuth{CredentialsFile: "/tmp/x.creds", NKeySeedFile: "/tmp/x.nk"},
		},
		{
			name: "password and nkey seed",
			auth: ConnAuth{Password: "p", NKeySeedFile: "/tmp/x.nk"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.auth.options()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "exactly one")
		})
	}
}

// A username with no password is one mechanism, not two, and must be accepted:
// some servers authenticate on the user alone.
func TestConnAuthAcceptsSingleMechanism(t *testing.T) {
	cases := []struct {
		name string
		auth ConnAuth
	}{
		{name: "nothing configured", auth: ConnAuth{}},
		{name: "username only", auth: ConnAuth{Username: "u"}},
		{name: "username and password", auth: ConnAuth{Username: "u", Password: "p"}},
		{name: "token only", auth: ConnAuth{Token: "t"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := tc.auth.options()

			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
}

// An unreadable seed file has to fail loudly at construction. Deferring it to
// connect time would report a confusing authentication error instead.
func TestConnAuthNKeySeedFileMustBeReadable(t *testing.T) {
	_, err := ConnAuth{NKeySeedFile: filepath.Join(t.TempDir(), "absent.nk")}.options()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "nkey seed file")
}

func TestConnAuthNKeySeedFileIsAccepted(t *testing.T) {
	// A syntactically valid user seed. Only its format matters here; it is
	// never used to reach a server.
	seed := "SUAGMJH5XLGZKQQWAWKRZJIGMOU4HPFUYLXJMXOO5NLFEO2OOQJ5LPRDPM"

	path := filepath.Join(t.TempDir(), "user.nk")
	require.NoError(t, os.WriteFile(path, []byte(seed), 0o600))

	opts, err := ConnAuth{NKeySeedFile: path}.options()

	require.NoError(t, err)
	assert.Len(t, opts, 1)
}

// TLS is applied only when a config is supplied, so an existing plaintext
// deployment is not silently switched over.
func TestConnectOptionsAppliesTLSOnlyWhenConfigured(t *testing.T) {
	l := zerolog.Nop()

	withoutTLS, err := connectOptions(&l, "test", ConnAuth{}, nil)
	require.NoError(t, err)

	withTLS, err := connectOptions(&l, "test", ConnAuth{}, &tls.Config{MinVersion: tls.VersionTLS13})
	require.NoError(t, err)

	assert.Len(t, withTLS, len(withoutTLS)+1,
		"a TLS config should add exactly one connection option")
}

// The auth error has to reach the caller rather than producing a connection
// that quietly uses the wrong credentials.
func TestConnectOptionsPropagatesAuthError(t *testing.T) {
	l := zerolog.Nop()

	_, err := connectOptions(&l, "test", ConnAuth{Token: "t", CredentialsFile: "/tmp/x.creds"}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestConnectOptionsAppendsExtras(t *testing.T) {
	l := zerolog.Nop()

	base, err := connectOptions(&l, "test", ConnAuth{}, nil)
	require.NoError(t, err)

	withExtra, err := connectOptions(&l, "test", ConnAuth{}, nil, natsgo.ReconnectBufSize(-1))
	require.NoError(t, err)

	assert.Len(t, withExtra, len(base)+1)
}
