package nats

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/logger"
)

// applyConnectOptions builds the options for url and c and applies them, so
// a test can read the resulting nats.go settings without a server.
func applyConnectOptions(t *testing.T, url string, c ConnConfig) (natsgo.Options, error) {
	t.Helper()

	l := logger.NewDefaultLogger("test")

	opts, err := connectOptions(&l, "test", url, "", "", c)
	if err != nil {
		return natsgo.Options{}, err
	}

	o := natsgo.GetDefaultOptions()

	for _, opt := range opts {
		require.NoError(t, opt(&o))
	}

	return o, nil
}

// writeCredsFile writes a creds file with a placeholder JWT and seed. nats.go
// reads the JWT when it builds the options, and the seed only to sign a server
// nonce, so neither has to be valid here.
func writeCredsFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "nats.creds")
	creds := "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.sig\n------END NATS USER JWT------\n\n" +
		"-----BEGIN USER NKEY SEED-----\nSUAPLACEHOLDER\n------END USER NKEY SEED------\n"
	require.NoError(t, os.WriteFile(path, []byte(creds), 0o600))

	return path
}

// writeCAFile writes a self-signed CA certificate, which nats.go parses when it
// builds the options.
func writeCAFile(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	return path
}

func TestConnectOptionsDefaults(t *testing.T) {
	o, err := applyConnectOptions(t, "nats://nats:4222", ConnConfig{})
	require.NoError(t, err)

	assert.Equal(t, "_INBOX_hatchet", o.InboxPrefix)
	assert.Equal(t, -1, o.MaxReconnect)
	assert.True(t, o.IgnoreAuthErrorAbort)
	assert.False(t, o.Secure)
	assert.False(t, o.TLSHandshakeFirst)
	assert.Nil(t, o.UserJWT)
	assert.Nil(t, o.RootCAsCB)
}

func TestConnectOptionsInboxPrefix(t *testing.T) {
	o, err := applyConnectOptions(t, "nats://nats:4222", ConnConfig{InboxPrefix: "_INBOX_other"})
	require.NoError(t, err)

	assert.Equal(t, "_INBOX_other", o.InboxPrefix)
}

func TestConnectOptionsCredsAndTLS(t *testing.T) {
	cases := []struct {
		name      string
		url       string
		creds     bool
		ca        bool
		conn      ConnConfig
		wantErr   bool
		wantFirst bool
	}{
		{
			name:  "creds over a tls url use the system roots",
			url:   "tls://nats:4222",
			creds: true,
		},
		{
			name:  "creds over a tls cluster url list",
			url:   "tls://a:4222, TLS://b:4222",
			creds: true,
		},
		{
			name:  "a CA file makes a nats url require tls",
			url:   "nats://nats:4222",
			creds: true,
			ca:    true,
		},
		{
			name:      "tls first makes a nats url require tls",
			url:       "nats://nats:4222",
			creds:     true,
			conn:      ConnConfig{TLSFirst: true},
			wantFirst: true,
		},
		{
			name:    "creds over plaintext are refused",
			url:     "nats://nats:4222",
			creds:   true,
			wantErr: true,
		},
		{
			name:    "one plaintext url in a cluster list is refused",
			url:     "tls://a:4222,nats://b:4222",
			creds:   true,
			wantErr: true,
		},
		{
			name:  "allow plaintext lifts the refusal",
			url:   "nats://nats:4222",
			creds: true,
			conn:  ConnConfig{AllowPlaintext: true},
		},
		{
			name: "a CA file alone needs no creds",
			url:  "tls://nats:4222",
			ca:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.creds {
				tc.conn.CredsFile = writeCredsFile(t)
			}

			if tc.ca {
				tc.conn.CAFile = writeCAFile(t)
			}

			o, err := applyConnectOptions(t, tc.url, tc.conn)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "not TLS")
				return
			}

			require.NoError(t, err)

			assert.Equal(t, tc.creds, o.UserJWT != nil, "user JWT callback")
			assert.Equal(t, tc.creds, o.SignatureCB != nil, "signature callback")
			assert.Equal(t, tc.ca, o.RootCAsCB != nil, "root CAs callback")
			assert.Equal(t, tc.ca || tc.wantFirst, o.Secure, "secure")
			assert.Equal(t, tc.wantFirst, o.TLSHandshakeFirst, "tls first")
		})
	}
}

func TestNewJetStreamDomain(t *testing.T) {
	// RetryOnFailedConnect returns a connection in the reconnecting state, so
	// the JetStream client can be built without a server.
	nc, err := natsgo.Connect("nats://127.0.0.1:1", natsgo.RetryOnFailedConnect(true), natsgo.MaxReconnects(0))
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	js, err := newJetStream(nc, "hub")
	require.NoError(t, err)
	assert.Equal(t, "hub", js.Options().Domain)

	js, err = newJetStream(nc, "")
	require.NoError(t, err)
	assert.Empty(t, js.Options().Domain)
}
