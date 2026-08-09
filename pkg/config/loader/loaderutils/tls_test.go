//go:build !e2e && !load && !rampup && !integration

package loaderutils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/config/shared"
)

func TestParseTLSMinVersion(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    uint16
		wantErr bool
	}{
		{name: "empty defaults to TLS 1.3", input: "", want: tls.VersionTLS13},
		{name: "1.3", input: "1.3", want: tls.VersionTLS13},
		{name: "tls1.3", input: "tls1.3", want: tls.VersionTLS13},
		{name: "TLS1.3 uppercase", input: "TLS1.3", want: tls.VersionTLS13},
		{name: "tls_1.3", input: "tls_1.3", want: tls.VersionTLS13},
		{name: "tls13", input: "tls13", want: tls.VersionTLS13},
		{name: "1.2", input: "1.2", want: tls.VersionTLS12},
		{name: "tls1.2", input: "tls1.2", want: tls.VersionTLS12},
		{name: "TLS1.2 uppercase", input: "TLS1.2", want: tls.VersionTLS12},
		{name: "tls_1.2", input: "tls_1.2", want: tls.VersionTLS12},
		{name: "tls12", input: "tls12", want: tls.VersionTLS12},
		{name: "leading/trailing whitespace", input: "  1.2  ", want: tls.VersionTLS12},
		{name: "1.1 rejected", input: "1.1", wantErr: true},
		{name: "1.0 rejected", input: "1.0", wantErr: true},
		{name: "arbitrary string rejected", input: "latest", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTLSMinVersion(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported TLS minimum version")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestLoadBaseTLSConfig_DefaultMinVersion(t *testing.T) {
	cfg := &shared.TLSConfigFile{}
	res, _, err := LoadBaseTLSConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS13), res.MinVersion)
}

func TestLoadBaseTLSConfig_ConfiguredMinVersion(t *testing.T) {
	cfg := &shared.TLSConfigFile{TLSMinVersion: "1.2"}
	res, _, err := LoadBaseTLSConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), res.MinVersion)
}

func TestLoadBaseTLSConfig_InvalidMinVersion(t *testing.T) {
	cfg := &shared.TLSConfigFile{TLSMinVersion: "1.0"}
	_, _, err := LoadBaseTLSConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported TLS minimum version")
}

func TestTLS12Handshake(t *testing.T) {
	cert := generateSelfSignedCert(t)

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MaxVersion:   tls.VersionTLS12,
		MinVersion:   tls.VersionTLS12,
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	require.NoError(t, err)
	defer ln.Close()

	addr := ln.Addr().String()

	t.Run("TLS 1.3 client fails against TLS 1.2 server", func(t *testing.T) {
		serverDone := acceptAndClose(ln)

		conn, err := tls.Dial("tcp", addr, &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: true, //nolint:gosec // self-signed test cert
		})
		if err == nil {
			conn.Close()
			t.Fatal("expected handshake failure with TLS 1.3 minimum against TLS 1.2 server")
		}

		<-serverDone
	})

	t.Run("TLS 1.2 client succeeds against TLS 1.2 server", func(t *testing.T) {
		serverDone := acceptAndClose(ln)

		conn, err := tls.Dial("tcp", addr, &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // self-signed test cert
		})
		require.NoError(t, err)
		conn.Close()

		<-serverDone
	})
}

// acceptAndClose accepts one TLS connection and closes done when the server
// goroutine exits, allowing the test to wait for handshake cleanup.
func acceptAndClose(ln net.Listener) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			_ = tlsConn.Handshake()
		}
		conn.Close()
	}()
	return done
}

func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  key,
	}
}

// writeCertPair writes a self-signed pair into dir, overwriting whatever is
// there. serial tells one generation from the next.
func writeCertPair(t *testing.T, dir string, serial int64) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))

	return certPath, keyPath
}

func serialOf(t *testing.T, cert *tls.Certificate) int64 {
	t.Helper()

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	return leaf.SerialNumber.Int64()
}

// expireThrottle makes the next get() re-stat, standing in for the wall clock
// passing reloadInterval.
func expireThrottle(r *certReloader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checked = time.Time{}
}

func TestCertReloaderPicksUpRotation(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertPair(t, dir, 1)

	r := &certReloader{certFile: certPath, keyFile: keyPath}

	first, err := r.get()
	require.NoError(t, err)
	assert.Equal(t, int64(1), serialOf(t, first))

	// Rotate in place, the way cert-manager rewrites a mounted Secret. The
	// mtime is set forward explicitly so the test does not depend on the
	// filesystem's timestamp granularity.
	writeCertPair(t, dir, 2)
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(certPath, future, future))

	cached, err := r.get()
	require.NoError(t, err)
	assert.Equal(t, int64(1), serialOf(t, cached),
		"a rotation inside the throttle window should not be observed yet")

	expireThrottle(r)

	rotated, err := r.get()
	require.NoError(t, err)
	assert.Equal(t, int64(2), serialOf(t, rotated), "the rotated pair should be served after the throttle expires")
}

func TestCertReloaderKeepsLastGoodPairThroughAFailedRead(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertPair(t, dir, 1)

	r := &certReloader{certFile: certPath, keyFile: keyPath}

	first, err := r.get()
	require.NoError(t, err)

	// A rotation is not atomic from the reader's side: the path can be briefly
	// gone. Failing here would turn a renewal into an outage.
	require.NoError(t, os.Remove(certPath))
	expireThrottle(r)

	again, err := r.get()
	require.NoError(t, err)
	assert.Equal(t, serialOf(t, first), serialOf(t, again))
}

func TestCertReloaderReportsAFirstLoadFailure(t *testing.T) {
	dir := t.TempDir()

	r := &certReloader{
		certFile: filepath.Join(dir, "absent.crt"),
		keyFile:  filepath.Join(dir, "absent.key"),
	}

	_, err := r.get()
	require.Error(t, err, "with nothing cached there is no pair to fall back to")
}

func TestLoadBaseTLSConfig_FileCertServesThroughTheReloader(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertPair(t, dir, 7)

	res, _, err := LoadBaseTLSConfig(&shared.TLSConfigFile{TLSCertFile: certPath, TLSKeyFile: keyPath})
	require.NoError(t, err)

	// Both, because this config is used for a server and for an mTLS client.
	require.NotNil(t, res.GetCertificate)
	require.NotNil(t, res.GetClientCertificate)

	got, err := res.GetCertificate(&tls.ClientHelloInfo{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), serialOf(t, got))

	fromClient, err := res.GetClientCertificate(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), serialOf(t, fromClient))
}

func TestLoadBaseTLSConfig_InlineCertHasNoReloader(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeCertPair(t, dir, 3)

	certPEM, err := os.ReadFile(certPath)
	require.NoError(t, err)

	keyPEM, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	res, _, err := LoadBaseTLSConfig(&shared.TLSConfigFile{
		TLSCert: string(certPEM),
		TLSKey:  string(keyPEM),
	})
	require.NoError(t, err)

	assert.Nil(t, res.GetCertificate, "inline PEM cannot change under a running process")
	assert.Nil(t, res.GetClientCertificate)
	require.Len(t, res.Certificates, 1)
}

func TestLoadBaseTLSConfig_MissingCertFileFailsAtStartup(t *testing.T) {
	dir := t.TempDir()

	_, _, err := LoadBaseTLSConfig(&shared.TLSConfigFile{
		TLSCertFile: filepath.Join(dir, "absent.crt"),
		TLSKeyFile:  filepath.Join(dir, "absent.key"),
	})
	require.Error(t, err, "a bad path should fail at load, not at the first handshake")
}
