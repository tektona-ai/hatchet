package loaderutils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/config/client"
	"github.com/hatchet-dev/hatchet/pkg/config/shared"
)

// reloadInterval bounds how often a handshake re-stats the certificate on
// disk. Renewal happens on the order of weeks, so checking once a minute picks
// one up promptly without putting a stat in front of every handshake.
const reloadInterval = time.Minute

// certReloader serves a key pair read from disk and picks up a replacement
// without a restart.
//
// An issuer rotates the files in place — cert-manager rewrites the mounted
// Secret, and the kubelet swaps the projected files under the process. Reading
// the pair once at startup means the process keeps presenting the old
// certificate for as long as it runs. That outlives the certificate: past its
// expiry every handshake fails at once, on a process that has otherwise been
// healthy for months.
type certReloader struct {
	certFile string
	keyFile  string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
	checked time.Time
}

// get returns the current key pair, re-reading it when the file on disk has
// changed since the last read.
//
// Once a pair has been loaded it is never dropped on error. A rotation is not
// atomic from the reader's side: the path can be briefly missing or hold a
// certificate whose key has not landed yet, and failing the handshake for that
// window would turn a renewal into an outage.
func (r *certReloader) get() (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	if r.cert != nil && now.Sub(r.checked) < reloadInterval {
		return r.cert, nil
	}

	info, err := os.Stat(r.certFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil
		}

		return nil, fmt.Errorf("could not stat TLS certificate %s: %w", r.certFile, err)
	}

	r.checked = now

	if r.cert != nil && info.ModTime().Equal(r.modTime) {
		return r.cert, nil
	}

	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		if r.cert != nil {
			return r.cert, nil
		}

		return nil, fmt.Errorf("could not load TLS key pair: %w", err)
	}

	r.cert = &cert
	r.modTime = info.ModTime()

	return r.cert, nil
}

// ParseTLSMinVersion parses a TLS minimum version string.
// Empty defaults to TLS 1.3.
func ParseTLSMinVersion(s string) (uint16, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "1.3", "tls1.3", "tls_1.3", "tls13":
		return tls.VersionTLS13, nil
	case "1.2", "tls1.2", "tls_1.2", "tls12":
		return tls.VersionTLS12, nil
	default:
		return 0, fmt.Errorf("unsupported TLS minimum version %q: must be \"1.2\" or \"1.3\"", s)
	}
}

func LoadClientTLSConfig(tlsConfig *client.ClientTLSConfigFile, serverName string) (*tls.Config, error) {
	res, ca, err := LoadBaseTLSConfig(&tlsConfig.Base)

	if err != nil {
		return nil, err
	}

	res.ServerName = serverName

	switch tlsConfig.Base.TLSStrategy {
	case "tls":
		if ca != nil {
			res.RootCAs = ca
		}
	case "mtls":
		res.ServerName = tlsConfig.TLSServerName
		res.RootCAs = ca
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid TLS strategy: %s", tlsConfig.Base.TLSStrategy)
	}

	return res, nil
}

func LoadServerTLSConfig(tlsConfig *shared.TLSConfigFile) (*tls.Config, error) {
	res, ca, err := LoadBaseTLSConfig(tlsConfig)

	if err != nil {
		return nil, err
	}

	switch tlsConfig.TLSStrategy {
	case "tls":
		res.ClientAuth = tls.VerifyClientCertIfGiven

		if ca != nil {
			res.ClientCAs = ca
		}
	case "mtls":
		if ca == nil {
			return nil, fmt.Errorf("Client CA is required for mTLS")
		}

		res.ClientAuth = tls.RequireAndVerifyClientCert
		res.ClientCAs = ca
	default:
		return nil, fmt.Errorf("invalid TLS strategy: %s", tlsConfig.TLSStrategy)
	}

	return res, nil
}

func LoadBaseTLSConfig(tlsConfig *shared.TLSConfigFile) (*tls.Config, *x509.CertPool, error) {
	var x509Cert tls.Certificate
	var err error

	// Set only for the file-based pair: inline PEM cannot change under a
	// running process, so there is nothing to reload.
	var reloader *certReloader

	switch {
	case tlsConfig.TLSCert != "" && tlsConfig.TLSKey != "":
		x509Cert, err = tls.X509KeyPair([]byte(tlsConfig.TLSCert), []byte(tlsConfig.TLSKey))
	case tlsConfig.TLSCertFile != "" && tlsConfig.TLSKeyFile != "":
		reloader = &certReloader{certFile: tlsConfig.TLSCertFile, keyFile: tlsConfig.TLSKeyFile}

		var cert *tls.Certificate

		// Load once here so a bad path or an unreadable pair fails at startup
		// rather than at the first handshake.
		if cert, err = reloader.get(); err == nil {
			x509Cert = *cert
		}
	}

	// This error used to go unread: the next assignment to err is
	// ParseTLSMinVersion below, which overwrites it. A certificate that was
	// configured but could not be parsed produced a config with no certificate
	// at all, and the failure only showed up as a handshake error later.
	if err != nil {
		return nil, nil, fmt.Errorf("could not load TLS certificate: %w", err)
	}

	var caBytes []byte

	switch {
	case tlsConfig.TLSRootCA != "":
		caBytes = []byte(tlsConfig.TLSRootCA)
	case tlsConfig.TLSRootCAFile != "":
		caBytes, err = os.ReadFile(tlsConfig.TLSRootCAFile)
	}

	var ca *x509.CertPool

	if len(caBytes) != 0 {
		ca = x509.NewCertPool()

		if ok := ca.AppendCertsFromPEM(caBytes); !ok {
			return nil, nil, fmt.Errorf("could not append root CA to cert pool: %w", err)
		}
	}

	minVersion, err := ParseTLSMinVersion(tlsConfig.TLSMinVersion)
	if err != nil {
		return nil, nil, err
	}

	res := &tls.Config{
		MinVersion: minVersion,
	}

	if len(x509Cert.Certificate) != 0 {
		res.Certificates = []tls.Certificate{x509Cert}
	}

	// Both callbacks, because this config is used for a server and for an mTLS
	// client: a server takes the certificate from GetCertificate, a client from
	// GetClientCertificate, and each ignores the other. Certificates stays set
	// so anything inspecting the config still sees the pair; it is only
	// consulted when a callback returns nothing.
	if reloader != nil {
		res.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return reloader.get()
		}
		res.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return reloader.get()
		}
	}

	return res, ca, nil
}
